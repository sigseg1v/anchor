package main

import (
	"log"
	"net"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type Client struct {
	id           uint64
	conn         net.Conn
	server       *Server
	room         *Room
	team         *Team
	state        string     // Client state, current scene, etc.
	sceneNum     int64      // last-observed scene; sceneIdNone if no save loaded. Read-locked under mu.
	mu           sync.Mutex // Mutex for safely updating state
	lastActivity time.Time
}

func (c *Client) handlePacket(packet string) {
	c.lastActivity = time.Now()

	packetType := gjson.Get(packet, "type").String()

	if !c.server.quietMode.Load() && !gjson.Get(packet, "quiet").Exists() {
		log.Printf("Client %d -> Server: %s\n", c.id, packetType)
	}

	if packetType == "UPDATE_CLIENT_STATE" {
		newSaveLoaded := gjson.Get(packet, "state.isSaveLoaded").Bool()
		newSceneRaw := gjson.Get(packet, "state.sceneNum").Int()
		newScene := sceneIdNone
		if newSaveLoaded {
			newScene = newSceneRaw
		}

		c.mu.Lock()
		c.state = gjson.Get(packet, "state").Raw
		c.state, _ = sjson.Set(c.state, "clientId", c.id)
		oldScene := c.sceneNum
		c.sceneNum = newScene
		c.mu.Unlock()

		team := c.room.findOrCreateTeam(gjson.Get(packet, "state.teamId").String())

		c.team = team

		if oldScene != newScene {
			c.room.onClientSceneTransition(c, oldScene, newScene)
		}
	}

	if packetType == "GAME_COMPLETE" {
		c.server.gameCompleteCount.Add(1)
	}

	if packetType == "UPDATE_RUPEES" {
		delta := gjson.Get(packet, "delta").Int()
		seed := gjson.Get(packet, "seed").Int()
		total := c.room.applyRupeesDelta(delta, seed)
		out, _ := sjson.Set(`{"type":"RUPEES_SET"}`, "total", total)
		c.room.broadcastPacketAll(out)
		return
	}

	if packetType == "FOLIAGE_DESTROY" {
		sceneNum := gjson.Get(packet, "sceneNum").Int()
		foliageId := gjson.Get(packet, "foliageId").String()
		if foliageId == "" {
			return
		}
		if c.room.addDestroyedFoliage(sceneNum, foliageId) {
			c.room.broadcastPacket(packet)
		}
		return
	}

	// ROCK_DESTROY always broadcasts. The set membership is just so that
	// joiners learn about destroyed rocks via ROCK_SNAPSHOT, but a destroy
	// after a lift still needs to reach peers so they can clear the held
	// overhead visual on the originator's dummy player. Receivers handle
	// destroying an already-killed rock idempotently.
	if packetType == "ROCK_DESTROY" {
		sceneNum := gjson.Get(packet, "sceneNum").Int()
		rockId := gjson.Get(packet, "rockId").String()
		if rockId == "" {
			return
		}
		c.room.addDestroyedRock(sceneNum, rockId)
		c.room.broadcastPacket(packet)
		return
	}

	// ROCK_LIFT goes into the same destroyed-rocks set as ROCK_DESTROY so
	// joiners get a unified ROCK_SNAPSHOT. We dedup the lift broadcast
	// itself (a rock can only be lifted once) but a later DESTROY for the
	// same rockId still broadcasts -- see the ROCK_DESTROY branch above.
	if packetType == "ROCK_LIFT" {
		sceneNum := gjson.Get(packet, "sceneNum").Int()
		rockId := gjson.Get(packet, "rockId").String()
		if rockId == "" {
			return
		}
		if c.room.addDestroyedRock(sceneNum, rockId) {
			c.room.broadcastPacket(packet)
		}
		return
	}

	// ITEM_SPAWN replicates collectible drops (currently from rock smashes).
	// The originating client sends with the actual rolled item params so
	// every peer ends up with the same drop type at the same position; we
	// just relay without state.
	if packetType == "ITEM_SPAWN" {
		c.room.broadcastPacket(packet)
		return
	}

	// ITEM_COLLECT despawns a previously-spawned EnItem00 across peers
	// (player picked it up, or its lifetime timer ran out). Stateless
	// relay; spawned items are ephemeral so no server-side bookkeeping
	// is needed for joiners (anything still alive will time out).
	if packetType == "ITEM_COLLECT" {
		c.room.broadcastPacket(packet)
		return
	}

	targetClientId := gjson.Get(packet, "targetClientId")

	if targetClientId.Exists() {
		value, ok := c.room.clients.Load(targetClientId.Uint())
		if ok {
			targetClient := value.(*Client)
			targetClient.sendPacket(packet)
		}
		return
	}

	targetTeamId := gjson.Get(packet, "targetTeamId")

	if packetType == "REQUEST_TEAM_STATE" {
		if !targetTeamId.Exists() {
			return
		}

		team := c.room.findOrCreateTeam(targetTeamId.String())
		teamMemberOnline := false
		c.room.clients.Range(func(_, value interface{}) bool {
			client := value.(*Client)
			client.mu.Lock()
			if client.id != c.id && client.conn != nil && client.team == team && gjson.Get(client.state, "isSaveLoaded").Bool() {
				teamMemberOnline = true
			}
			client.mu.Unlock()
			return true
		})

		if teamMemberOnline {
			team.mu.Lock()
			team.clientIdsRequestingState = append(team.clientIdsRequestingState, c.id)
			team.mu.Unlock()
			team.broadcastPacket(packet)
			return
		}

		// Teammate is offline, see if we have a saved state for the team
		outgoingPacket := `{"type": "UPDATE_TEAM_STATE"}`
		team.mu.Lock()
		if team.state != "{}" {
			outgoingPacket, _ = sjson.SetRaw(outgoingPacket, "state", team.state)
		}
		outgoingPacket, _ = sjson.Set(outgoingPacket, "queue", team.queue)
		team.mu.Unlock()

		c.sendPacket(outgoingPacket)
	} else if packetType == "UPDATE_TEAM_STATE" {
		if !targetTeamId.Exists() {
			return
		}

		team := c.room.findOrCreateTeam(targetTeamId.String())

		team.mu.Lock()
		clientIdsRequestingState := team.clientIdsRequestingState
		team.state = gjson.Get(packet, "state").Raw
		team.queue = []string{}
		team.clientIdsRequestingState = []uint64{}
		team.mu.Unlock()

		for _, clientId := range clientIdsRequestingState {
			if value, ok := c.room.clients.Load(clientId); ok {
				client := value.(*Client)
				client.sendPacket(packet)
			}
		}

	} else if packetType == "UPDATE_ROOM_STATE" {
		c.room.mu.Lock()
		c.room.state = gjson.Get(packet, "state").Raw
		c.room.mu.Unlock()
		c.room.broadcastPacket(packet)
	} else if targetTeamId.Exists() {
		team := c.room.findOrCreateTeam(targetTeamId.String())
		addToQueue := gjson.Get(packet, "addToQueue")

		if addToQueue.Exists() && addToQueue.Bool() {
			team.mu.Lock()
			team.queue = append(team.queue, packet)
			team.mu.Unlock()
		}

		team.broadcastPacket(packet)
	} else {
		c.room.broadcastPacket(packet)
	}
}

func (c *Client) sendPacket(packet string) {
	if !c.server.quietMode.Load() && !gjson.Get(packet, "quiet").Exists() {
		log.Printf("Client %d <- Server: %s\n", c.id, gjson.Get(packet, "type").String())
	}

	// Lock to prevent race condition with disconnect
	c.mu.Lock()
	conn := c.conn
	if conn == nil {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	// Set write deadline to prevent blocking on dead connections
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := conn.Write(append([]byte(packet), 0))
	conn.SetWriteDeadline(time.Time{}) // Clear deadline

	if err != nil {
		c.disconnect()
	} else {
		c.mu.Lock()
		c.lastActivity = time.Now()
		c.mu.Unlock()
	}
}

func (c *Client) disconnect() {
	if c.conn != nil {
		c.conn.Close()
	}

	c.mu.Lock()
	c.state, _ = sjson.Set(c.state, "online", false)
	c.state, _ = sjson.Set(c.state, "isSaveLoaded", false)
	c.conn = nil
	c.sceneNum = sceneIdNone
	c.mu.Unlock()

	c.server.onlineClients.Delete(c.id)

	if c.room != nil {
		c.room.onClientDisconnect(c.id)
	}
}

// sendRupeesSnapshot sends the current room rupee total to a
// (re)connecting client. Skips the send until at least one client has
// seeded the room -- before that, the joiner's own first delta is
// what bootstraps the count, and pushing 0 here would clobber their
// save-loaded balance back to zero.
func (c *Client) sendRupeesSnapshot() {
	if c.conn == nil || c.room == nil {
		return
	}
	total, ok := c.room.snapshotRupees()
	if !ok {
		return
	}
	packet, _ := sjson.Set(`{"type":"RUPEES_SET"}`, "total", total)
	c.sendPacket(packet)
}

// sendFoliageSnapshotForScene sends the destroyed-foliage set for one
// scene, packaged as a single FOLIAGE_SNAPSHOT packet. Called when
// the client transitions into a scene so the arriving client can hide
// previously cut grass on entry.
func (c *Client) sendFoliageSnapshotForScene(sceneNum int64) {
	if c.conn == nil || c.room == nil {
		return
	}
	ids := c.room.snapshotFoliageForScene(sceneNum)
	if ids == nil {
		return
	}
	packet, _ := sjson.Set(`{"type":"FOLIAGE_SNAPSHOT"}`, "sceneNum", sceneNum)
	packet, _ = sjson.Set(packet, "foliageIds", ids)
	c.sendPacket(packet)
}

// sendRockSnapshotForScene mirrors sendFoliageSnapshotForScene for
// rocks: hands a (re)connecting / scene-entering client the set of
// rocks already smashed in that scene so they can hide them on entry.
func (c *Client) sendRockSnapshotForScene(sceneNum int64) {
	if c.conn == nil || c.room == nil {
		return
	}
	ids := c.room.snapshotRocksForScene(sceneNum)
	if ids == nil {
		return
	}
	packet, _ := sjson.Set(`{"type":"ROCK_SNAPSHOT"}`, "sceneNum", sceneNum)
	packet, _ = sjson.Set(packet, "rockIds", ids)
	c.sendPacket(packet)
}

// sendSceneAuthoritiesSnapshot sends the room's current per-scene
// authority assignments so a (re)connecting client can populate its
// local map without waiting for elections triggered by other peers.
func (c *Client) sendSceneAuthoritiesSnapshot() {
	if c.conn == nil || c.room == nil {
		return
	}
	snap := c.room.snapshotSceneAuthorities()
	for sceneNum, authId := range snap {
		packet, _ := sjson.Set(`{"type":"SCENE_AUTHORITY"}`, "sceneNum", sceneNum)
		packet, _ = sjson.Set(packet, "authorityClientId", authId)
		c.sendPacket(packet)
	}
}

func (c *Client) sendRoomState() {
	if c.conn == nil {
		return
	}

	c.room.mu.Lock()
	packet, _ := sjson.SetRaw(`{"type":"UPDATE_ROOM_STATE"}`, "state", c.room.state)
	c.room.mu.Unlock()

	c.sendPacket(packet)
}
