package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

var ClientNodeName string
var ClientNodePort string
var ClientIpAddress, _ = getIp()

var ServerMap map[string]net.Conn
var ServerNum int

var ipAddress, _ = getIp()
var beginStatus bool
var decisionVote int
var commitFinalVote int
var madeDecision bool

func main() {

	configFileName := ""
	beginStatus = false
	ServerNum = 0
	decisionVote = 0
	madeDecision = false
	ServerMap = make(map[string]net.Conn)
	//	./client asdf config.txt
	if len(os.Args) > 1 {
		ClientNodeName = os.Args[1]
	}

	if len(os.Args) > 2 {
		configFileName = os.Args[2]
		readConfig(configFileName)
		for len(ServerMap) < ServerNum {
			d, _ := time.ParseDuration("0.05s")
			time.Sleep(d)
		}
	} else {
		err := "no config file."
		fmt.Println("Error: ", err)
		return
	}

	inputReader := bufio.NewReader(os.Stdin)
	go ioScanner(inputReader)

	// // address := ipAddress + ":" + localNodePort
	// address := ipAddress + ":1111"

	// listener, err := net.Listen("tcp", address)
	// fmt.Println("-----start listening-----", "client: ", ClientNodeName, ", local address:", address)
	// if err != nil {
	// 	fmt.Println("Error listening :", err.Error())
	// 	return
	// }

	// // accept tcp connection from other nodes
	// for {
	// 	fmt.Println("receiver")
	// 	conn, listenErr := listener.Accept()
	// 	if listenErr != nil {
	// 		fmt.Println("error: accepting tcp connection:", listenErr.Error())
	// 		return
	// 	}
	// 	fmt.Println("Connection accepted, remote address = ", conn.RemoteAddr())
	// 	go handleConnection(conn)
	// }
	for {
		if len(ServerMap) == ServerNum {
			for _, v := range ServerMap {
				go handleConnection(v)
			}
		}
	}

}

func readConfig(configFileName string) {
	configFile, configError := os.Open(configFileName)
	if configError != nil {
		fmt.Println("An error occurred on opening the config file: ", configError)
		return
	}
	configReader := bufio.NewReader(configFile)
	for {
		//A fa21-cs425-g01-01.cs.illinois.edu 1234
		temp, _, err := configReader.ReadLine()
		if err == io.EOF {
			fmt.Println(err)
			return
		}
		nodeMessage := string(temp)
		fmt.Println("server message :" + nodeMessage)
		ServerNum++
		tempMessage := strings.Fields(nodeMessage)
		branchName := tempMessage[0]
		hostname := DNSResolution(tempMessage[1])
		port := tempMessage[2]
		go tryDial(branchName, hostname, port)
	}
}

func tryDial(serverName string, hostname string, port string) {
	for {
		conn, sendingErr := net.Dial("tcp", hostname+":"+port)
		if sendingErr != nil {
			fmt.Println(sendingErr)
			continue
		}
		ServerMap[serverName] = conn
		return
	}
}

func ioScanner(inputReader *bufio.Reader) {
	for {
		// read message from stdin
		temp, _, err := inputReader.ReadLine()
		if err == io.EOF {
			fmt.Println("An error occurred on reading stdin.")
			return
		}
		input := string(temp)
		fmt.Println("read stdin:", input)
		processInput(input)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		reader := bufio.NewReader(conn)
		for {
			msg, _, err := reader.ReadLine()
			if err != nil {
				fmt.Println("Error reading from: ", err.Error())
				return
			}
			processReply(string(msg))
		}
	}
}

func processReply(msg string) {
	if msg == "BEGIN-OK" {
		decisionVote++
		if decisionVote == ServerNum {
			madeDecision = true
			fmt.Println("OK")
		}
	} else if msg == "BEGIN-ABORT" {
		decisionVote++
		if decisionVote == ServerNum {
			madeDecision = true
			fmt.Println("ABORTED")
		}

	} else if msg == "COMMIT-OK" {
		commitFinalVote++
		if commitFinalVote == ServerNum {
			fmt.Println("COMMIT OK")
			madeDecision = true
		}
	} else if msg == "COMMIT-ABORTED" {
		commitFinalVote++
		if commitFinalVote == ServerNum {
			fmt.Println("ABORTED")
			madeDecision = true
		}
	} else if msg == "COMMIT-REPLY-ABORT" {
		decisionVote = -1
		commitFinalVote = 0
		sendToAllServer("COMMIT-DECISION-ABORT")
	} else if msg == "COMMIT-REPLY-OK" {
		if decisionVote != -1 {
			decisionVote++
		}
		if decisionVote == ServerNum {
			commitFinalVote = 0
			sendToAllServer("COMMIT-DECISION-OK")
		}

	} else if msg == "NOT-FOUND,-ABORTED" {
		decisionVote = 0
		sendToAllServer("TRANSACTION-ABORT")
	} else if msg == "TRANSACTION-ABORTED" {
		decisionVote++
		if decisionVote == ServerNum {
			fmt.Println("NOT-FOUND,-ABORTED")
			madeDecision = true
			beginStatus = false
		}

	} else if msg == "ABORTED" {
		decisionVote++
		if decisionVote == ServerNum {
			madeDecision = true
			fmt.Println("ABORTED")
		}
	} else {
		fmt.Println(msg)
		madeDecision = true
	}
}

func processInput(msg string) {
	message := strings.Split(msg, " ")
	msgType := message[0]
	madeDecision = false
	decisionVote = 0
	fmt.Println("test msgType: ", msgType)

	if msgType == "BEGIN" {
		if beginStatus == false {
			sendToAllServer("BEGIN")
			for {
				if madeDecision {
					break
				}
			}
			beginStatus = true
		} else {
			sendToAllServer("BEGIN-ABORT")
			for {
				if madeDecision {
					break
				}
			}
			beginStatus = false
		}
		return
	} else if !beginStatus {
		return
	}

	if msgType == "ABORT" {
		sendToAllServer("ABORT")
		for {
			if madeDecision {
				break
			}
		}
		beginStatus = false
		return
	}

	if msgType == "COMMIT" {
		sendToAllServer("COMMIT")
		for {
			if madeDecision {
				break
			}
		}
		beginStatus = false
		return
	}

	//DEPOSIT A.foo 20
	dest := strings.Split(message[1], ".")
	server := dest[0]

	if msgType == "BALANCE" {
		decisionVote = 0
		send(msg, ServerMap[server])
		for {
			if madeDecision {
				break
			}
		}
		return
	}
	if msgType == "DEPOSIT" {
		send(msg, ServerMap[server])
		for {
			if madeDecision {
				break
			}
		}
		return
	}
	if msgType == "WITHDRAW" {
		send(msg, ServerMap[server])
		for {
			if madeDecision {
				break
			}
		}
		return
	}
}

func sendToAllServer(input string) {
	for _, v := range ServerMap {
		send(input, v)
	}
}

func sendToOtherServer(input string, conn net.Conn) {
	for _, v := range ServerMap {
		send(input, v)
	}
}


func send(input string, conn net.Conn) {
	msg := ClientNodeName + " " + input + "\n"
	conn.Write([]byte(msg))
	fmt.Println("test send :" , msg)
}

func getIp() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), err
			}
		}
	}
	return "", errors.New("can not find localhost ip address")
}

func DNSResolution(name string) string {
	hostIP, err := net.ResolveIPAddr("ip4", name)
	if err != nil {
		fmt.Println("DNS resolution error.")
	}
	return hostIP.String()
}
