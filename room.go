package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// sceneIdNone marks a client as not currently in any scene (e.g. file
// select). Mirrors SCENE_ID_MAX (0x6E) on the client side; we use -1 in
// our internal tracking to keep "no scene" out of the int64 sceneNum
// keyspace.
const sceneIdNone int64 = -1

type Room struct {
	id      string
	clients sync.Map
	teams   sync.Map
	state   string     // Room Settings
	mu      sync.Mutex // Mutex for safely updating state

	// Per-scene authority. Key is sceneNum (int64), value is the
	// owning clientId (uint64). Authority is "first connected client
	// to enter that scene" and persists until they leave or
	// disconnect, at which point we elect the next remaining client
	// in that scene (lowest clientId tiebreaker). Mutex serialises
	// the read-modify-write of a single scene's authority across
	// concurrent UPDATE_CLIENT_STATE handlers.
	sceneAuthorities map[int64]uint64
	authMu           sync.Mutex

	// Shared room rupee count. The first connected client seeds it
	// from their save (rupeesInitialized = true on first
	// UPDATE_RUPEES delta we accept) and from then on every client's
	// pickups/spends adjust this single value, which the server
	// broadcasts back as RUPEES_SET. Old anchor builds simply ignore
	// the new packet types, and clients without the matching
	// feature flag never produce them, so the room defaults to 0.
	rupees            int64
	rupeesInitialized bool
	rupeesMu          sync.Mutex

	// Per-scene foliage destruction set. Key is sceneNum (int64),
	// value is the set of foliage IDs (string of "actorId:params:x:y:z")
	// destroyed in that scene. Clients send FOLIAGE_DESTROY when they
	// cut a foliage actor; the server adds to the set and broadcasts
	// to the room so other peers in the same scene can kill the
	// matching actor. When a client transitions into a scene we send
	// them a FOLIAGE_SNAPSHOT of the set so they can hide already-
	// destroyed foliage on entry.
	foliageDestroyed map[int64]map[string]bool
	foliageMu        sync.Mutex

	// Per-scene destroyed-rock set. Same encoding/lifecycle as
	// foliageDestroyed but for liftable/breakable rocks (currently
	// ACTOR_EN_ISHI). Clients send ROCK_DESTROY when a rock smashes
	// locally; the server records and rebroadcasts, and ROCK_SNAPSHOT
	// hands the set to clients on scene entry.
	rockDestroyed map[int64]map[string]bool
	rockMu        sync.Mutex
}

func NewRoom(id string, ownerClientId uint64, packet string) *Room {
	roomState, _ := sjson.Set(gjson.Get(packet, "roomState").Raw, "ownerClientId", ownerClientId)

	return &Room{
		id:               id,
		clients:          sync.Map{},
		teams:            sync.Map{},
		state:            roomState,
		sceneAuthorities: make(map[int64]uint64),
		foliageDestroyed: make(map[int64]map[string]bool),
		rockDestroyed:    make(map[int64]map[string]bool),
	}
}

func (r *Room) findOrCreateTeam(teamId string) *Team {
	var team *Team
	value, ok := r.teams.Load(teamId)
	if ok {
		team = value.(*Team)
	} else {
		team = &Team{
			id:    teamId,
			state: "{}",
			room:  r,
			queue: make([]string, 0),
		}
		r.teams.Store(teamId, team)
	}

	return team
}

func (r *Room) broadcastPacket(packet string) {
	clientId := gjson.Get(packet, "clientId").Uint()

	r.clients.Range(func(_, value interface{}) bool {
		client := value.(*Client)
		if client.conn != nil && client.id != clientId {
			go func(c *Client) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Panic in sendPacket for client %d: %v", c.id, r)
					}
				}()
				c.sendPacket(packet)
			}(client)
		}

		return true
	})
}

func (r *Room) broadcastAllClientState() {
	packet := `{"type":"ALL_CLIENT_STATE","state":[]}`

	idToIndex := make(map[interface{}]int)
	index := 0

	r.clients.Range(func(id, value interface{}) bool {
		client := value.(*Client)
		idToIndex[id] = index
		client.mu.Lock()
		packet, _ = sjson.SetRaw(packet, "state."+fmt.Sprint(index), client.state)
		client.mu.Unlock()
		index++
		return true
	})

	r.clients.Range(func(id, value interface{}) bool {
		client := value.(*Client)
		if client.conn == nil {
			return true
		}

		clientPacket, _ := sjson.Set(packet, "state."+fmt.Sprint(idToIndex[id])+".self", true)

		go func(c *Client, p string) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Panic in sendPacket for client %d: %v", c.id, r)
				}
			}()
			c.sendPacket(p)
		}(client, clientPacket)

		return true
	})
}

// broadcastPacketAll sends to every connected client in the room,
// including the originator. Used for authoritative server-originated
// announcements (SCENE_AUTHORITY) where the trigger client also needs
// the result.
func (r *Room) broadcastPacketAll(packet string) {
	r.clients.Range(func(_, value interface{}) bool {
		client := value.(*Client)
		if client.conn != nil {
			go func(c *Client) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Panic in sendPacket for client %d: %v", c.id, r)
					}
				}()
				c.sendPacket(packet)
			}(client)
		}
		return true
	})
}

// sendSceneAuthorityPacket builds and broadcasts a SCENE_AUTHORITY
// announcement. authorityClientId == 0 means "no current authority"
// (everyone has left the scene).
func (r *Room) sendSceneAuthorityPacket(sceneNum int64, authorityClientId uint64) {
	packet, _ := sjson.Set(`{"type":"SCENE_AUTHORITY"}`, "sceneNum", sceneNum)
	packet, _ = sjson.Set(packet, "authorityClientId", authorityClientId)
	r.broadcastPacketAll(packet)
}

// electSceneAuthority picks the lowest-clientId client currently in the
// given scene with a save loaded and a live connection. Returns 0 if
// no client qualifies. Caller must hold r.authMu.
func (r *Room) electSceneAuthority(sceneNum int64, exclude uint64) uint64 {
	var winner uint64
	r.clients.Range(func(_, value interface{}) bool {
		client := value.(*Client)
		if client.id == exclude {
			return true
		}
		client.mu.Lock()
		conn := client.conn
		scene := client.sceneNum
		state := client.state
		client.mu.Unlock()
		if conn == nil || scene != sceneNum {
			return true
		}
		if !gjson.Get(state, "isSaveLoaded").Bool() {
			return true
		}
		if winner == 0 || client.id < winner {
			winner = client.id
		}
		return true
	})
	return winner
}

// onClientSceneTransition reconciles authority when a client moves
// between scenes (or loads/unloads a save). Call with the *new* effective
// scene (sceneIdNone for "no scene") AFTER updating the client's stored
// scene/state so re-elections see the post-transition truth.
func (r *Room) onClientSceneTransition(client *Client, oldScene, newScene int64) {
	if oldScene == newScene {
		return
	}

	r.authMu.Lock()
	defer r.authMu.Unlock()

	// Leaving the old scene: if this client was authority, re-elect.
	if oldScene != sceneIdNone {
		if curr, ok := r.sceneAuthorities[oldScene]; ok && curr == client.id {
			next := r.electSceneAuthority(oldScene, client.id)
			if next == 0 {
				delete(r.sceneAuthorities, oldScene)
				r.sendSceneAuthorityPacket(oldScene, 0)
			} else {
				r.sceneAuthorities[oldScene] = next
				r.sendSceneAuthorityPacket(oldScene, next)
			}
		}
	}

	// Entering the new scene: if no authority exists yet, this client
	// becomes authority. Otherwise the existing authority stays --
	// "first to enter" wins, and re-entries do not steal authority.
	if newScene != sceneIdNone {
		currentAuth, hasAuth := r.sceneAuthorities[newScene]
		if !hasAuth {
			r.sceneAuthorities[newScene] = client.id
			r.sendSceneAuthorityPacket(newScene, client.id)
		} else {
			// Echo the existing authority back to the entering
			// client so they don't have to fall back to the "default
			// to self-authority" path while waiting on a broadcast
			// that's not coming. Without this, two clients entering
			// the same scene roughly simultaneously would each
			// assume authority and double-broadcast initial enemy
			// spawns.
			packet, _ := sjson.Set(`{"type":"SCENE_AUTHORITY"}`, "sceneNum", newScene)
			packet, _ = sjson.Set(packet, "authorityClientId", currentAuth)
			go client.sendPacket(packet)
			// Tell the existing authority that a peer just joined
			// their scene so it can push an ENEMY_FULL_SNAPSHOT to
			// the new arrival. Skip when the entering client *is*
			// the authority (a no-op).
			if currentAuth != client.id {
				if value, ok := r.clients.Load(currentAuth); ok {
					authClient := value.(*Client)
					notify, _ := sjson.Set(`{"type":"PEER_ENTERED_SCENE"}`, "sceneNum", newScene)
					notify, _ = sjson.Set(notify, "peerClientId", client.id)
					go authClient.sendPacket(notify)
				}
			}
		}
		// Hand off the destroyed-foliage set for this scene so the
		// arriving client hides anything previously cut.
		go client.sendFoliageSnapshotForScene(newScene)
		// Same for already-smashed rocks.
		go client.sendRockSnapshotForScene(newScene)
	}
}

// onClientDisconnect re-elects authority for every scene this client
// owned. Called after the client's conn has been cleared so elections
// won't pick them again.
func (r *Room) onClientDisconnect(clientId uint64) {
	r.authMu.Lock()
	defer r.authMu.Unlock()

	var lostScenes []int64
	for sceneNum, authId := range r.sceneAuthorities {
		if authId == clientId {
			lostScenes = append(lostScenes, sceneNum)
		}
	}
	for _, sceneNum := range lostScenes {
		next := r.electSceneAuthority(sceneNum, clientId)
		if next == 0 {
			delete(r.sceneAuthorities, sceneNum)
			r.sendSceneAuthorityPacket(sceneNum, 0)
		} else {
			r.sceneAuthorities[sceneNum] = next
			r.sendSceneAuthorityPacket(sceneNum, next)
		}
	}
}

// applyRupeesDelta updates the shared room rupee count by `delta` (which
// may be negative), seeds the count on first call from the originator's
// pre-change wallet (carried in `baseline` so the room starts at the
// joiner's existing balance instead of 0), and returns the new total.
// `seed` is the client's pre-delta local wallet -- used only the very
// first time we see a delta in this room. Caller broadcasts the result.
func (r *Room) applyRupeesDelta(delta int64, seed int64) int64 {
	r.rupeesMu.Lock()
	defer r.rupeesMu.Unlock()
	if !r.rupeesInitialized {
		// First contact wins: take the joiner's pre-delta balance as
		// the room baseline so we don't start fresh rooms at 0 if a
		// client connects mid-run.
		r.rupees = seed
		r.rupeesInitialized = true
	}
	r.rupees += delta
	if r.rupees < 0 {
		r.rupees = 0
	}
	return r.rupees
}

// snapshotRupees returns the current room rupee total and whether it
// has been seeded yet. New connections receive RUPEES_SET only after
// the room is initialized; before that, joiners contribute their own
// wallet via the first UPDATE_RUPEES delta.
func (r *Room) snapshotRupees() (int64, bool) {
	r.rupeesMu.Lock()
	defer r.rupeesMu.Unlock()
	return r.rupees, r.rupeesInitialized
}

// addDestroyedFoliage records a foliage cut and returns true if this
// is the first time we've seen this id (so the caller knows whether
// to broadcast). De-duping at the server avoids fanning out the same
// destroy to every peer when two clients independently cut the same
// piece of grass during a brief authority handoff.
func (r *Room) addDestroyedFoliage(sceneNum int64, foliageId string) bool {
	r.foliageMu.Lock()
	defer r.foliageMu.Unlock()
	set, ok := r.foliageDestroyed[sceneNum]
	if !ok {
		set = make(map[string]bool)
		r.foliageDestroyed[sceneNum] = set
	}
	if set[foliageId] {
		return false
	}
	set[foliageId] = true
	return true
}

// snapshotFoliageForScene returns a copy of destroyed foliage ids for
// the given scene, or nil if nothing is recorded.
func (r *Room) snapshotFoliageForScene(sceneNum int64) []string {
	r.foliageMu.Lock()
	defer r.foliageMu.Unlock()
	set, ok := r.foliageDestroyed[sceneNum]
	if !ok || len(set) == 0 {
		return nil
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	return ids
}

// addDestroyedRock mirrors addDestroyedFoliage for rock actors. Returns
// true on first insertion so the caller knows whether to broadcast.
func (r *Room) addDestroyedRock(sceneNum int64, rockId string) bool {
	r.rockMu.Lock()
	defer r.rockMu.Unlock()
	set, ok := r.rockDestroyed[sceneNum]
	if !ok {
		set = make(map[string]bool)
		r.rockDestroyed[sceneNum] = set
	}
	if set[rockId] {
		return false
	}
	set[rockId] = true
	return true
}

// snapshotRocksForScene returns a copy of destroyed rock ids for the
// given scene, or nil if nothing is recorded.
func (r *Room) snapshotRocksForScene(sceneNum int64) []string {
	r.rockMu.Lock()
	defer r.rockMu.Unlock()
	set, ok := r.rockDestroyed[sceneNum]
	if !ok || len(set) == 0 {
		return nil
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	return ids
}

// snapshotSceneAuthorities returns a copy of the current authority map
// for sending to a (re)connecting client so they don't have to wait
// for elections to learn pre-existing assignments.
func (r *Room) snapshotSceneAuthorities() map[int64]uint64 {
	r.authMu.Lock()
	defer r.authMu.Unlock()
	out := make(map[int64]uint64, len(r.sceneAuthorities))
	for k, v := range r.sceneAuthorities {
		out[k] = v
	}
	return out
}

func (r *Room) GetLastActivity() time.Time {
	var lastActivity time.Time

	r.clients.Range(func(id, value interface{}) bool {
		client := value.(*Client)
		if client.lastActivity.After(lastActivity) {
			lastActivity = client.lastActivity
		}
		return true
	})

	return lastActivity
}
