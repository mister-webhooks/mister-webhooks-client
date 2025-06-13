package client

import (
	"context"
	"fmt"

	"github.com/jessekempf/kafka-go"
)

// An Admin supports running administrative commands and queries.
type Admin struct {
	kafkaAdmin *kafka.Client
}

func NewAdmin(ctx context.Context, profile *ConnectionProfile) (*Admin, error) {
	a := &kafka.Client{
		Addr: profile.broker,
		Transport: &kafka.Transport{
			ClientID: "admin-client",
			TLS:      profile.tls,
			SASL:     profile.auth,
		},
	}

	_, err := a.ApiVersions(ctx, &kafka.ApiVersionsRequest{})

	if err != nil {
		return nil, err
	}

	return &Admin{
		kafkaAdmin: a,
	}, nil
}

type Topic[T any] struct {
	Name          string
	PartitionData []T
}

func (t *Topic[T]) TopicPartitions() map[string][]int {
	partitionIds := make([]int, 0, len(t.PartitionData))

	for i := range t.PartitionData {
		partitionIds[i] = i
	}

	return map[string][]int{
		t.Name: partitionIds,
	}
}

type Group[T any] struct {
	Id      string
	State   string
	Members []kafka.DescribeGroupsResponseMember
	Data    T
}

type PartitionStat struct {
	FirstOffset int64
	LastOffset  int64
}

func (a *Admin) StatTopics(ctx context.Context) ([]Topic[PartitionStat], error) {
	meta, err := a.kafkaAdmin.Metadata(ctx, &kafka.MetadataRequest{})

	if err != nil {
		return nil, err
	}

	lor := &kafka.ListOffsetsRequest{
		Topics:         make(map[string][]kafka.OffsetRequest),
		IsolationLevel: kafka.ReadCommitted,
	}

	for _, topic := range meta.Topics {
		if topic.Error != nil {
			return nil, fmt.Errorf("error listing topic %s: %w", topic.Name, err)
		}

		for _, partition := range topic.Partitions {
			lor.Topics[topic.Name] = append(lor.Topics[topic.Name], kafka.FirstOffsetOf(partition.ID), kafka.LastOffsetOf(partition.ID))
		}
	}

	offsetResponse, err := a.kafkaAdmin.ListOffsets(ctx, lor)

	if err != nil {
		return nil, err
	}

	topicStats := make([]Topic[PartitionStat], 0, len(lor.Topics))

	for name, info := range offsetResponse.Topics {
		partitionStats := make([]PartitionStat, len(info))

		for _, offsets := range info {
			partitionStats[offsets.Partition] = PartitionStat{
				FirstOffset: offsets.FirstOffset,
				LastOffset:  offsets.LastOffset,
			}

			if offsets.Error != nil {
				return nil, err
			}
		}

		topicStats = append(topicStats, Topic[PartitionStat]{
			Name:          name,
			PartitionData: partitionStats,
		})
	}

	return topicStats, nil
}

func (a *Admin) StatGroups(ctx context.Context) ([]Group[map[string][]int64], error) {
	resp, err := a.kafkaAdmin.ListGroups(ctx, &kafka.ListGroupsRequest{})

	if err != nil {
		return nil, err
	}

	if resp.Error != nil {
		return nil, resp.Error
	}

	groups := make([]string, len(resp.Groups))

	for i, g := range resp.Groups {
		groups[i] = g.GroupID
	}

	dgresp, err := a.kafkaAdmin.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
		GroupIDs: groups,
	})

	if err != nil {
		return nil, err
	}

	knownGroups := make(map[string]Group[any], len(dgresp.Groups))

	for _, group := range dgresp.Groups {
		if group.Error != nil {
			return nil, err
		}

		knownGroups[group.GroupID] = Group[any]{
			Id:      group.GroupID,
			State:   group.GroupState,
			Members: group.Members,
		}
	}

	consumerGroups := make([]Group[map[string][]int64], len(groups))

	for i, groupId := range groups {
		r, err := a.kafkaAdmin.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
			GroupID: groupId,
		})

		if err != nil {
			return nil, err
		}

		consumerGroups[i] = Group[map[string][]int64]{
			Id:      groupId,
			State:   knownGroups[groupId].State,
			Members: knownGroups[groupId].Members,
			Data:    map[string][]int64{},
		}

		for topic, partitionInfo := range r.Topics {
			consumerGroups[i].Data[topic] = make([]int64, len(partitionInfo))

			for i, info := range partitionInfo {
				if info.Error != nil {
					return nil, info.Error
				}

				consumerGroups[i].Data[topic][info.Partition] = info.CommittedOffset
			}
		}

	}

	return consumerGroups, nil
}
