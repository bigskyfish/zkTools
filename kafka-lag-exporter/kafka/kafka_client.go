package kafka

import (
	"github.com/Shopify/sarama"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	metaDataRefreshTime    = 5 * time.Second
	topicOffsetRefreshTime = 1 * time.Second
	groupRefreshTime       = 2 * time.Second
)

type availableOffset struct {
	err    bool
	offset int64
}

type broker struct {
	id   int32
	addr string
	br   *sarama.Broker
}

type partDetails struct {
	part     int32
	leaderId int32
}

type metaData struct {
	topicsInError       []string
	topicPartNotok      map[string][]int32
	topicPartOk         map[string][]*partDetails
	topicPartOffsets    map[string]map[int32]*availableOffset
	lastPartOffsetFetch time.Time
	groupOffsets        map[string]map[string]map[int32]int64
	groupMembers        map[string]int
	lastGroupFetch      time.Time
	brokers             []*broker
	lastFetch           time.Time
	fetchSuccess        bool
}

type kafkaClient struct {
	brokerList     []string
	requiredTopics []string
	requiredGroups map[string][]string
	client         sarama.Client
	conf           *sarama.Config
	mateDate       *metaData
	lock           *sync.Mutex
}

func _getBrokersFromMetadata(mrd *sarama.MetadataResponse) (brokers []*broker) {
	brokers = make([]*broker, 0)
	for _, br := range mrd.Brokers {
		brokers = append(brokers, &broker{id: br.ID(), addr: br.Addr(), br: br})
	}
	return
}

func (cl *kafkaClient) _getMetadata() *metaData {
	if cl.mateDate != nil && time.Now().Sub(cl.mateDate.lastFetch) < metaDataRefreshTime {
		return cl.mateDate
	}
	cl.mateDate = nil
	numBrokers := len(cl.client.Brokers())
	var br *sarama.Broker
	attempts := 0
	refreshAttempts := 0
	var err error = nil
	for {
		for {
			br = cl.client.Brokers()[rand.Intn(numBrokers)]
			err := br.Open(cl.conf)
			if err == nil || err == sarama.ErrAlreadyConnected {
				break
			}
			attempts++
			if attempts == 10 {
				log.Printf("Error: Couldn't connect to any broker")
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if err == nil || err == sarama.ErrAlreadyConnected {
			break
		}
		if refreshAttempts < 6 {
			time.Sleep(200 * time.Millisecond)
			cl.client.RefreshMetadata(cl.requiredTopics...)
			refreshAttempts++
			attempts = 0
		} else {
			log.Printf("Error: Broker connection problem")
			return nil
		}
	}

	defer br.Close()
	mrd, err := br.GetMetadata(&sarama.MetadataRequest{Topics: cl.requiredTopics})
	if err != nil {
		log.Printf("Metadata fetch error :%v", err)
		return nil
	}
	cl.mateDate = &metaData{topicPartNotok: make(map[string][]int32),
		topicPartOk: make(map[string][]*partDetails), topicsInError: make([]string, 0)}
	cl.mateDate.lastFetch = time.Now()
	cl.mateDate.brokers = _getBrokersFromMetadata(mrd)
	for _, tmd := range mrd.Topics {
		if tmd.Err != sarama.ErrNoError {
			cl.mateDate.topicsInError = append(cl.mateDate.topicsInError, tmd.Name)
			continue
		}
		for _, metadata := range tmd.Partitions {
			if metadata.Err != sarama.ErrNoError {
				_, ok := cl.mateDate.topicPartNotok[tmd.Name]
				if !ok {
					cl.mateDate.topicPartNotok[tmd.Name] = []int32{metadata.ID}
				} else {
					cl.mateDate.topicPartNotok[tmd.Name] = append(cl.mateDate.topicPartNotok[tmd.Name],
						metadata.ID)
				}
			} else {
				_, ok := cl.mateDate.topicPartOk[tmd.Name]
				if !ok {
					cl.mateDate.topicPartOk[tmd.Name] = []*partDetails{&partDetails{metadata.ID, metadata.Leader}}
				} else {
					cl.mateDate.topicPartOk[tmd.Name] = append(cl.mateDate.topicPartOk[tmd.Name],
						&partDetails{metadata.ID, metadata.Leader})
				}
			}
		}
	}
	cl.mateDate.fetchSuccess = true
	return cl.mateDate
}

func _populateGroupOffsets(group string, offsetDetails *sarama.OffsetFetchResponse,
	groupOffsets map[string]map[string]map[int32]int64) {
	if len(offsetDetails.Blocks) == 0 {
		return
	}
	groupDetails := groupOffsets[group]
	if groupDetails == nil {
		groupDetails = make(map[string]map[int32]int64)
		groupOffsets[group] = groupDetails
	}
	for topic, partResponse := range offsetDetails.Blocks {
		groupDetails[topic] = make(map[int32]int64)
		for part, offRes := range partResponse {
			if offRes.Err == sarama.ErrNoError {
				groupDetails[topic][part] = offRes.Offset
			} else {
				groupDetails[topic][part] = -1
			}
		}
	}
}

func (cl *kafkaClient) _getGroupOffsets() {
	cl._getMetadata()
	if cl.mateDate == nil {
		return
	}
	if time.Now().Sub(cl.mateDate.lastGroupFetch) < groupRefreshTime {
		return
	}
	groupOffsets := make(map[string]map[string]map[int32]int64)
	cl.mateDate.groupOffsets = groupOffsets
	cl.mateDate.groupMembers = make(map[string]int)
	addElem := 0
	var offReq *sarama.OffsetFetchRequest
	for group, topics := range cl.requiredGroups {
		offReq = &sarama.OffsetFetchRequest{ConsumerGroup: group, Version: 1}
		for _, topic := range topics {
			okayPartitions, ok := cl.mateDate.topicPartOk[topic]
			if ok {
				for _, okayPartition := range okayPartitions {
					offReq.AddPartition(topic, okayPartition.part)
				}
				addElem++
			}
		}
		if addElem > 0 {
			addElem = 0
			coordinatorBroker, err := cl.client.Coordinator(group)
			if err != nil {
				log.Printf("Offset fetch, error: %v", err)
				continue
			}
			descGrs := &sarama.DescribeGroupsRequest{Groups: []string{group}}
			resp, err := coordinatorBroker.DescribeGroups(descGrs)
			if err == nil && resp != nil {
				cl.mateDate.groupMembers[group] = len(resp.Groups[0].Members)
			}
			offsetDetails, err := coordinatorBroker.FetchOffset(offReq)
			if err == nil {
				_populateGroupOffsets(group, offsetDetails, cl.mateDate.groupOffsets)
			}
		}
	}
	cl.mateDate.lastGroupFetch = time.Now()
}

func (cl *kafkaClient) _getTopicOffsets() {
	brs := cl.client.Brokers()
	if len(brs) == 0 {
		log.Printf("No available brokers to fetch metdata")
		return
	}
	idToBr := make(map[int32]*sarama.Broker)
	for _, br := range brs {
		idToBr[br.ID()] = br
	}
	offResponses := make(map[int32]*sarama.OffsetRequest)
	for topic, partitions := range cl.mateDate.topicPartOk {
		for _, partition := range partitions {
			offResp := offResponses[partition.leaderId]
			if offResp == nil {
				offResp = &sarama.OffsetRequest{Version: int16(0)}
				offResponses[partition.leaderId] = offResp
			}
			offResp.AddBlock(topic, partition.part, -1, 1)
		}
	}
	var (
		err     error
		offResp *sarama.OffsetResponse
		offset  int64
	)

	offRespAll := make([]*sarama.OffsetResponse, 0)
	for bride, offRes := range offResponses {
		br, ok := idToBr[bride]
		if !ok {
			log.Printf("Broker for id %d not found to fetch metadata", bride)
			continue
		}
		err = br.Open(cl.conf)
		if err != nil && err != sarama.ErrAlreadyConnected {
			log.Printf("Error in connecting to broker:%s %v", br.Addr(), err)
			continue
		}
		offResp, err = br.GetAvailableOffsets(offRes)
		br.Close()
		if err != nil {
			log.Printf("Error in getting offset %v", err)
			continue
		}
		offRespAll = append(offRespAll, offResp)
	}
	if len(offRespAll) == 0 {
		log.Printf("No topic offset fetch happened")
		return
	}
	cl.mateDate.topicPartOffsets = make(map[string]map[int32]*availableOffset)
	for _, offResp = range offRespAll {
		version := offResp.Version
		for topic, block := range offResp.Blocks {
			_, ok := cl.mateDate.topicPartOffsets[topic]
			if !ok {
				cl.mateDate.topicPartOffsets[topic] = make(map[int32]*availableOffset)
			}
			for partition, ofrBlock := range block {
				if version == 0 {
					offset = ofrBlock.Offsets[0]
				} else {
					offset = ofrBlock.Offset
				}
				cl.mateDate.topicPartOffsets[topic][partition] = &availableOffset{err: ofrBlock.Err != sarama.ErrNoError, offset: offset}
			}
		}
	}
	cl.mateDate.lastPartOffsetFetch = time.Now()
}

func (cl *kafkaClient) getGroupOffsets() map[string]map[string]map[int32]int64 {
	cl.lock.Lock()
	defer cl.lock.Unlock()
	cl._getGroupOffsets()
	ret := make(map[string]map[string]map[int32]int64)
	if cl.mateDate == nil {
		return ret
	}
	for group, topics := range cl.mateDate.groupOffsets {
		ret[group] = make(map[string]map[int32]int64)
		for topic, parts := range topics {
			ret[group][topic] = make(map[int32]int64)
			for part, offset := range parts {
				ret[group][topic][part] = offset
			}
		}
	}
	return ret
}

func (cl *kafkaClient) getTopicOffsets() (ret map[string]map[int32]int64) {
	ret = make(map[string]map[int32]int64)
	cl.lock.Lock()
	defer cl.lock.Unlock()
	mData := cl._getMetadata()
	if mData == nil {
		log.Printf("Metadata fetch errror")
		return
	}
	if time.Now().Sub(cl.mateDate.lastPartOffsetFetch) > topicOffsetRefreshTime {
		cl._getTopicOffsets()
	}
	for topic, partOffset := range cl.mateDate.topicPartOffsets {
		ret[topic] = make(map[int32]int64)
		for part, offset := range partOffset {
			if offset.err {
				continue
			}
			out, ok := ret[topic]
			if !ok {
				out = make(map[int32]int64)
				ret[topic] = out
			}
			out[part] = offset.offset
		}
	}
	return
}
