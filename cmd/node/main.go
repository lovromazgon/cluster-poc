package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"github.com/lovromazgon/cluster-poc"
)

func main() {
	var (
		peers    peers
		serfPort int
		id       string
	)

	flag.Func(
		"peers", "Comma separated list of host:port pairs (127.0.0.1:1111,127.0.0.1:2222)",
		func(s string) error {
			return peers.UnmarshalText([]byte(s))
		},
	)
	flag.IntVar(&serfPort, "serf-port", 1111, "Port used by Serf.")
	flag.StringVar(&id, "id", "", "ID of the node (defaults to the node address).")

	flag.Parse()
	if err := mainE(id, peers, serfPort); err != nil {
		panic(err)
	}
}

func mainE(id string, peers peers, serfPort int) (err error) {
	self := peer{
		host:     "localhost",
		serfPort: serfPort,
		raftPort: serfPort + 1,
	}

	dataDir := fmt.Sprintf("data-%s", id)
	if id == "" {
		id = self.serfAddr()
		dataDir = fmt.Sprintf("data-%d", serfPort)
	}

	err = os.MkdirAll(dataDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create data directory %q: %w", dataDir, err)
	}

	fmt.Printf("I am %v: %+v\n", id, self)
	fmt.Printf("My peers are: %+v\n", peers)

	// Setup Raft

	r, err := cluster.CreateRaft(id, self.host, self.raftPort, dataDir, &cluster.FSM{})
	if err != nil {
		return fmt.Errorf("failed to create raft: %w", err)
	}

	defer func() {
		leader := r.VerifyLeader()
		if leader.Error() == nil {
			fmt.Println("I am the leader, removing myself before I stop")
			// we are the leader, remove ourselves
			future := r.RemoveServer(raft.ServerID(self.raftAddr()), 0, 0)
			if err := future.Error(); err != nil {
				// log
				fmt.Printf("removing raft server failed: %s\n", err)
			}
		}

		fmt.Println("Shutting down Raft ...")
		future := r.Shutdown()
		futureErr := future.Error()
		if futureErr != nil {
			futureErr = fmt.Errorf("failed to shutdown Raft: %w", futureErr)
			err = errors.Join(err, futureErr)
		}
	}()

	// Setup Serf

	s, err := cluster.CreateSerf(id, self.host, self.serfPort, dataDir, peers.serfAddrs())
	if err != nil {
		return fmt.Errorf("failed to create serf: %w", err)
	}
	defer func() {
		tmpErr := s.Leave()
		if tmpErr != nil {
			tmpErr = fmt.Errorf("failed to leave serf: %w", tmpErr)
			err = errors.Join(err, tmpErr)
		}
		tmpErr = s.Shutdown()
		if tmpErr != nil {
			tmpErr = fmt.Errorf("failed to shutdown serf: %w", tmpErr)
			err = errors.Join(err, tmpErr)
		}
	}()

	// Start the event loop

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)

	ticker := time.NewTicker(3 * time.Second)

	in := make(chan []byte)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			in <- scanner.Bytes()
		}
	}()

	for {
		select {
		case <-ch:
			fmt.Println("shutting down")
			return nil
		case <-ticker.C:
			future := r.VerifyLeader()
			if err := future.Error(); err != nil {
				fmt.Println("node is a follower")
			} else {
				fmt.Println("node is a leader")
			}
		case cmd := <-in:
			leader := r.VerifyLeader()
			if leader.Error() != nil {
				fmt.Println("NOT supported on a follower node")
				continue
			}

			future := r.Apply(cmd, 10*time.Second)
			if err := future.Error(); err != nil {
				fmt.Printf("failed to apply command: %s\n", err)
				continue
			}
			fmt.Println("command applied successfully")
		case ev := <-s.EventCh:
			leader := r.VerifyLeader()
			if leader.Error() != nil {
				continue
			}

			if memberEvent, ok := ev.(serf.MemberEvent); ok {
				for _, member := range memberEvent.Members {
					eventType := memberEvent.EventType()
					peer := peer{
						host:     "localhost",
						serfPort: int(member.Port),
						raftPort: int(member.Port) + 1,
					}
					fmt.Printf("member event: %v (%+v)\n", eventType, peer)

					if eventType == serf.EventMemberJoin {
						future := r.AddVoter(raft.ServerID(peer.raftAddr()), raft.ServerAddress(peer.raftAddr()), 0, 0)
						if err := future.Error(); err != nil {
							// log
							fmt.Printf("adding raft voter failed: %s\n", err)
						}
					} else if eventType == serf.EventMemberLeave || eventType == serf.EventMemberFailed {
						fmt.Printf("removing raft server: %s\n", peer.raftAddr())
						future := r.RemoveServer(raft.ServerID(peer.raftAddr()), 0, 0)
						if err := future.Error(); err != nil {
							// log
							fmt.Printf("removing raft server failed: %s\n", err)
						}
					}
				}
			}
		}
	}
}

type peer struct {
	host     string
	serfPort int
	raftPort int
}

func (p peer) raftAddr() string {
	return fmt.Sprintf("%s:%d", p.host, p.raftPort)
}

func (p peer) serfAddr() string {
	return fmt.Sprintf("%s:%d", p.host, p.serfPort)
}

func (p *peer) UnmarshalText(text []byte) error {
	host, portRaw, found := strings.Cut(string(text), ":")
	if !found {
		return fmt.Errorf("invalid peer, needs to follow the format \"host:port\": %s", text)
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil {
		return fmt.Errorf("invalid port: %w", err)
	}
	p.host = host
	p.serfPort = port
	p.raftPort = port + 1
	return nil
}

type peers []peer

func (p peers) raftAddrs() []string {
	addrs := make([]string, len(p))
	for i, peer := range p {
		addrs[i] = peer.raftAddr()
	}
	return addrs
}

func (p peers) serfAddrs() []string {
	addrs := make([]string, len(p))
	for i, peer := range p {
		addrs[i] = peer.serfAddr()
	}
	return addrs
}

func (p *peers) UnmarshalText(text []byte) error {
	var out peers
	for i, peerText := range bytes.Split(text, []byte(",")) {
		var tmpPeer peer
		err := tmpPeer.UnmarshalText(peerText)
		if err != nil {
			return fmt.Errorf("peer %d invalid: %w", i, err)
		}
		out = append(out, tmpPeer)
	}
	*p = out
	return nil
}
