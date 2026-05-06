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
}

func NewRoom(id string, ownerClientId uint64, packet string) *Room {
	roomState, _ := sjson.Set(gjson.Get(packet, "roomState").Raw, "ownerClientId", ownerClientId)

	return &Room{
		id:               id,
		clients:          sync.Map{},
		teams:            sync.Map{},
		state:            roomState,
		sceneAuthorities: make(map[int64]uint64),
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
		if _, ok := r.sceneAuthorities[newScene]; !ok {
			r.sceneAuthorities[newScene] = client.id
			r.sendSceneAuthorityPacket(newScene, client.id)
		}
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
