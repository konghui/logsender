package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"gopkg.in/redis.v3"
	"os"
	"strings"
)

type Configuration struct {
	Monitor []nodeConfiguration
}

type nodeConfiguration struct {
	File        string
	Linehandler string
	Sendhandler string
	Redis       struct {
		Addr     string
		Password string
		Db       int64
		Channel  string
	}
}

type watcherNode struct {
	fileName    string
	currentSeek int64
	buffer      bytes.Buffer
	fd          *os.File
	serviceType string
	redisClient *redis.Client
	lineHandler func(string) string
	sendHandler func(string)
	nodeConfig  *nodeConfiguration
}

type myWatcher struct {
	watcherList   map[string]*watcherNode
	notifyWatcher *fsnotify.Watcher
	configFile    string
	config        *Configuration
}

const BUFF_SIZE = 1024
const LINE_SEP = '\n'

func main() {

	mywatcher := new(myWatcher)
	mywatcher.watcherList = make(map[string]*watcherNode)
	mywatcher.argsParser()
	if err := mywatcher.getConfigFromJson(); err != nil {
		perrAndExit("unable to get the config file", err, 2)
	}
	mywatcher.showRunConfig()

	if err := mywatcher.initWatcher(); err != nil {
		perrAndExit("init watcer error", err, 3)
	}
	mywatcher.mainLoop()

	// fd.Close, watcher.Close
}

func perrAndExit(msg string, err error, rc int) {
	fmt.Printf("%s:%s\n", err.Error())
	os.Exit(rc)
}

func usage() {

	fmt.Printf("usage:%s [config]\nif config file not given use ./conf.json instead\n")
	os.Exit(1)
}

func (w *myWatcher) argsParser() {
	if len(os.Args) > 2 {
		usage()
	} else if len(os.Args) == 2 {
		w.configFile = os.Args[1]

	} else {
		w.configFile = "config.json"
	}
}

// read the config from json file
func (w *myWatcher) getConfigFromJson() (err error) {
	var config Configuration
	file, err := os.Open(w.configFile)
	if err != nil {
		return
	}
	decoder := json.NewDecoder(file)
	if err = decoder.Decode(&config); err != nil {
		return
	}
	w.config = &config
	return
}

func (w *myWatcher) showRunConfig() {
	fmt.Println("use config:")
	for _, v := range w.config.Monitor {
		fmt.Printf("monitorfile=>%s, linehandler=>%s\nredis:\n\taddr=>%s, password=>%s, db=>%d, channel=>%s\n\n", v.File, v.Linehandler, v.Redis.Addr, v.Redis.Password, v.Redis.Db, v.Redis.Channel)
	}
}

func getParentDir(path string) (rv string) {
	for i := len(path) - 1; i > 0; i-- {
		if path[i] == '/' {
			rv = string(path[:i])
			return
		}
	}
	return
}

func (w *myWatcher) eventsHandler(event *fsnotify.Event) {
	node, ok := w.watcherList[event.Name]
	if ok {
		if event.Op&fsnotify.Write == fsnotify.Write {
			node.readLine()
		} else if event.Op&fsnotify.Create == fsnotify.Create {
			// if the new create file in the monitor list, readd it to the list and update the file information.
			w.register(w.watcherList[event.Name].nodeConfig)
			node.readLine()

		}
		w.addWatcher(event.Name)
	}
}

func (w *myWatcher) mainLoop() {
	for {
		select {
		case ev := <-w.notifyWatcher.Events:
			w.eventsHandler(&ev)
		case err := <-w.notifyWatcher.Errors:
			fmt.Printf("error:\n", err)
		}
	}
}

//read the line from a file, when read a line then call the callback function to hand the data
func (w *watcherNode) readLine() error {
	buffer := make([]byte, BUFF_SIZE)
	// the start index of the each line
	var begin_of_line int

	for {
		begin_of_line = 0
		n, err := w.fd.Read(buffer)
		if err != nil {
			fmt.Printf("read file error:%s\n", err.Error())
			return err
		}
		for i, v := range buffer[:n] {
			if v == LINE_SEP {
				w.sendHandler(w.lineHandler(string(buffer[begin_of_line:i])))

				begin_of_line = i + 1

			}
		}
		// move the offset of the file to the last line break
		w.currentSeek, err = w.fd.Seek(int64(begin_of_line-n), os.SEEK_CUR)
		if err != nil {
			fmt.Printf("Seek error!:%s\n", err.Error)
			return err
		}
	}
	return nil
}

func (n *watcherNode) lineHandlerRaw(line string) (rv string) {
	fmt.Println(line)
	rv = line
	return
}

func (n *watcherNode) lineHandlerNginx(line string) (rv string) {
	param := strings.Split(line, " ")
	responseTime := strings.TrimLeft(param[3], "[")
	responseCode := param[8]
	rv = fmt.Sprintf("time = %s, code = %s\n", responseTime, responseCode)
	return
}

func (w *myWatcher) addWatcher(fileName string) (err error) {
	if err = w.notifyWatcher.Add(fileName); err != nil {
		fmt.Printf("faild to add Watcher file %s:%s\n", fileName, err.Error())
		// if file not exist, use inotify to monitor file create
		if os.IsNotExist(err) {
			parentDir := getParentDir(fileName)
			w.addWatcher(parentDir)
			if v, ok := w.watcherList[fileName]; ok {
				v.fd.Close()
			}
		}
		return
	}
	return
}

func (w *myWatcher) register(nodeConfig *nodeConfiguration) (err error) {

	var node watcherNode
	fileName := nodeConfig.File
	// add the file to nitify:w
	w.addWatcher(fileName)

	node.fd, err = os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			parentDir := getParentDir(fileName)
			w.addWatcher(parentDir)
			if v, ok := w.watcherList[fileName]; ok {
				v.fd.Close()
			}
		}
		return
	}
	node.fileName = fileName

	if v, ok := w.watcherList[fileName]; ok { //file is been deleted and refreash the fd
		v.fd = node.fd

	} else { // first run it
		// link to the new node config
		node.nodeConfig = nodeConfig
		node.initSenderHandler()
		node.initLineHandler()
		w.watcherList[fileName] = &node
	}
	return
}

func (w *myWatcher) initWatcher() (err error) {
	w.notifyWatcher, err = fsnotify.NewWatcher()
	if err != nil {
		return
	}

	for i, _ := range w.config.Monitor {
		w.register(&w.config.Monitor[i])
	}
	return
}

func (n *watcherNode) initRedis() {
	option := redis.Options{Addr: n.nodeConfig.Redis.Addr, Password: n.nodeConfig.Redis.Password, DB: n.nodeConfig.Redis.Db}
	n.redisClient = redis.NewClient(&option)
	n.sendHandler = n.senderHandlerRedis
}

func (n *watcherNode) initSenderHandler() {
	switch n.nodeConfig.Sendhandler {
	case "redis":
		n.initRedis()
	default:
		fmt.Printf("Unknown SenderHandler %s\n", n.nodeConfig.Sendhandler)
		os.Exit(4)
	}
}

func (n *watcherNode) senderHandlerRedis(msg string) {
	n.redisClient.Publish(n.nodeConfig.Redis.Channel, msg)
}

func (n *watcherNode) initLineHandler() {
	switch n.nodeConfig.Linehandler {
	case "raw":
		n.initRaw()
	case "nginx":
		n.initNginx()
	default:
		fmt.Printf("Unknown LineHandler %s\n", n.nodeConfig.Linehandler)
		os.Exit(4)
	}
}

func (n *watcherNode) initRaw() {
	n.lineHandler = n.lineHandlerRaw
}

func (n *watcherNode) initNginx() {
	n.lineHandler = n.lineHandlerNginx
}
