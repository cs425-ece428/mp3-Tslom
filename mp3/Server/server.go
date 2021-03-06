package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var localNodeName string
var localNodePort string
var ipAddress, _ = getIp()
var accountMap map[string]int

// key = client name. value = dangling transactions
// delete the entry when a transaction is abort
var transactionRecordMap map[string][]accountRecord

type accountRecord struct {
	accountName string
	balance     int
}

//var clientMap map[string]net.Conn

// TODO for isolation, key = account name , val = client name - empty : no lock;
var readLockMap map[string][]string
var writeLockMap map[string]string

// var bufferedTransactionMap map[string][]string

func main() {
	accountMap = make(map[string]int)
	transactionRecordMap = make(map[string][]accountRecord)
	readLockMap = make(map[string][]string)
	writeLockMap = make(map[string]string)
	// bufferedTransactionMap = make(map[string][]string)

	//  ./server C config.txt
	if len(os.Args) > 1 {
		localNodeName = os.Args[1]
	}
	fmt.Println("server branch:", localNodeName)

	if len(os.Args) > 2 {
		configFileName := os.Args[2]
		readConfig(configFileName)
	} else {
		err := "no config file."
		fmt.Println("Error: ", err)
		return
	}

	address := ipAddress + ":" + localNodePort

	listener, err := net.Listen("tcp", address)
	fmt.Println("-----start listening-----", "branch: ", localNodeName, ", local address:", address)
	if err != nil {
		fmt.Println("Error listening :", err.Error())
		return
	}

	// accept tcp connection from other nodes
	for {
		fmt.Println("receiver")
		conn, listenErr := listener.Accept()
		if listenErr != nil {
			fmt.Println("error: accepting tcp connection:", listenErr.Error())
			return
		}
		fmt.Println("Connection accepted, remote address = ", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	// fmt.Println("test handleConnection")

	for {
		msg, _, err := reader.ReadLine()

		if err != nil {
			fmt.Println("Error reading from: ", err.Error())
			return
		}
		fmt.Println(string(msg))
		processMessage(string(msg), conn)
		// time.Sleep(time.Second * 1)
	}

}

func processMessage(msg string, conn net.Conn) {
	message := strings.Split(msg, " ")
	src := message[0]
	msgType := message[1]

	// fmt.Println("test msgType: ", msgType)
	//if _, ok := clientMap[src]; !ok {
	//	clientMap[src] = conn
	//}

	if msgType == "BEGIN" {
		reply(conn, "BEGIN-OK")
		return

	} else if msgType == "BEGIN-ABORT" {
		abort(src)
		reply(conn, "BEGIN-ABORTED")
		return

	} else if msgType == "COMMIT" {
		if isLegalTransactions(src) {
			reply(conn, "COMMIT-REPLY-OK")
		} else {
			reply(conn, "COMMIT-REPLY-ABORT")
		}
		return

	} else if msgType == "COMMIT-DECISION-OK" {
		delete(transactionRecordMap, src)
		releaseLock(src)
		reply(conn, "COMMIT-OK")
		return
	} else if msgType == "COMMIT-DECISION-ABORT" {
		abort(src)
		releaseLock(src)
		reply(conn, "COMMIT-ABORTED")
		return

	} else if msgType == "ABORT" {
		abort(src)
		releaseLock(src)
		reply(conn, "ABORTED")
		return
	} else if msgType == "TRANSACTION-ABORT" {
		abort(src)
		releaseLock(src)
		reply(conn, "TRANSACTION-ABORTED")
		return
	}

	BranchAccountName := message[2]
	if !strings.HasPrefix(BranchAccountName, localNodeName) {
		return
	}

	if msgType == "DEPOSIT" {
		accountName := message[2]
		m := message[3]
		money, _ := strconv.Atoi(m)
		addWriteLock(accountName, src)
		writeTransaction(src, accountName)
		if val, ok := accountMap[accountName]; ok {
			accountMap[accountName] = money + val
		} else {
			accountMap[accountName] = money
		}
		// fmt.Println("test DEPOSIT accountMap[accountName]: ", accountMap[accountName])
		reply(conn, "OK")
		return

	} else if msgType == "WITHDRAW" {
		accountName := message[2]
		m := message[3]
		money, _ := strconv.Atoi(m)
		addReadLock(accountName, src)
		if val, ok := accountMap[accountName]; ok {
			writeTransaction(src, accountName)
			accountMap[accountName] = val - money
			// fmt.Println("test WITHDRAW accountMap[accountName]: ", accountMap[accountName])
			reply(conn, "OK")
		} else {
			//abort(src)
			reply(conn, "NOT-FOUND,-ABORTED")
		}
		return

	} else if msgType == "BALANCE" {
		accountName := message[2]
		addReadLock(accountName, src)
		if val, ok := accountMap[accountName]; ok {
			re := accountName + " = " + strconv.Itoa(val)
			reply(conn, re)
		} else {
			//abort(src)
			reply(conn, "NOT-FOUND,-ABORTED")
		}
		return
	}
}

func abort(src string) {
	if _, ok := transactionRecordMap[src]; !ok {
		return
	}

	for _, record := range transactionRecordMap[src] {
		if record.balance == -2 {
			continue
		}
		if record.balance == -1 {
			delete(accountMap, record.accountName)
		} else {
			accountMap[record.accountName] = record.balance
		}
	}
	delete(transactionRecordMap, src)
}

func reply(conn net.Conn, msg string) {
	message := msg + "\n"
	conn.Write([]byte(message))
}

func isLegalTransactions(src string) bool {
	if _, ok := transactionRecordMap[src]; !ok {
		return true
	}

	for _, record := range transactionRecordMap[src] {
		if record.balance == -2 {
			continue
		}
		// fmt.Println("test accountMap[record.accountName]: ", accountMap[record.accountName])
		if accountMap[record.accountName] < 0 {
			return false
		}
	}
	return true
}

func writeTransaction(src string, accountName string) {
	if buffered, ok := transactionRecordMap[src]; ok {
		if containsAccount(buffered, accountName) {
			return
		}
		var record *accountRecord
		if val, ok := accountMap[accountName]; ok {
			record = &accountRecord{
				accountName,
				val,
			}
		} else {
			record = &accountRecord{
				accountName,
				-1,
			}
		}
		newBuffered := append(buffered, *record)
		transactionRecordMap[src] = newBuffered

	} else {
		var record *accountRecord
		if val, ok := accountMap[accountName]; ok {
			record = &accountRecord{
				accountName,
				val,
			}
		} else {
			record = &accountRecord{
				accountName,
				-1,
			}
		}
		newBuffered := []accountRecord{*record}
		transactionRecordMap[src] = newBuffered
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
		fmt.Println("node message:" + nodeMessage)

		tempMessage := strings.Fields(nodeMessage)
		branchName := tempMessage[0]
		if branchName == localNodeName {
			localNodePort = tempMessage[2]
			fmt.Println("completed reading from config file, multicast group loaded successfully")
			configFile.Close()
			return
		}
	}
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

func containsAccount(s []accountRecord, e string) bool {
	for _, a := range s {
		if a.accountName == e {
			return true
		}
	}
	return false
}

func isContainString(items []string, item string) int {
	for i, eachItem := range items {
		if eachItem == item {
			return i
		}
	}
	return -1
}

func addReadLock(accountName string, clientName string) {
	for {
		if _, ok := writeLockMap[accountName]; !ok {
			if _, ok := readLockMap[accountName]; !ok {
				newClientArr := []string{clientName}
				readLockMap[accountName] = newClientArr
				return
			} else if isContainString(readLockMap[accountName], clientName) != -1 {
				return
			} else {
				readLockMap[accountName] = append(readLockMap[accountName], clientName)
				return
			}
		} else if writeLockMap[accountName] == clientName {
			return
		}

		time.Sleep(time.Millisecond * 100)
	}
}

func addWriteLock(accountName string, clientName string) {
	for {
		if _, ok := writeLockMap[accountName]; !ok {
			if _, ok := readLockMap[accountName]; !ok {
				writeLockMap[accountName] = clientName
				return
			} else if len(readLockMap[accountName]) == 1 && isContainString(readLockMap[accountName], clientName) != -1 {
				writeLockMap[accountName] = clientName
				delete(readLockMap, accountName)
				return
			}
		} else if writeLockMap[accountName] == clientName {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func releaseLock(clientName string) {
	for account, clientList := range readLockMap {
		idx := isContainString(clientList, clientName)
		if idx != -1 {
			readLockMap[account] = append(readLockMap[account][:idx], readLockMap[account][(idx+1):]...)
		}
	}
	for account, client := range writeLockMap {
		if client == clientName {
			delete(writeLockMap, account)
		}
	}
}
