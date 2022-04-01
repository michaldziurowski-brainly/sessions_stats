package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gocarina/gocsv"
)

type SessionStats struct {
	ID                 string `csv:"id"`
	Market             string `csv:"market"`
	NoOfAssignAttempts int    `csv:"no_of_assign_attempts"`
	CreatedAt          string `csv:"created_at"`
	CreatedByRole      string `csv:"created_by_role"`
	RejectedAt         string `csv:"rejected_at"`
	RejectedReason     string `csv:"rejected_reason"`
	ClosedAt           string `csv:"closed_at"`
	ClosedReason       string `csv:"closed_reason"`
	ConfirmedAt        string `csv:"confirmed_at"`
}

type DynamoItem struct {
	ID        string `dynamodbav:"id"`
	Metadata  string `dynamodbav:"metadata"`
	CreatedAt string `dynamodbav:"createdAt"`
	Market    string `dynamodbav:"market"`
}

const (
	SessionMetadata                                      = "SESSION"
	SessionCreatedByUserEvent                            = "DOMAINEVENT#SessionCreatedByUser"
	SessionCreatedByTutorEvent                           = "DOMAINEVENT#SessionCreatedByTutor"
	SessionConfirmedByTutorEvent                         = "DOMAINEVENT#SessionConfirmedByTutor"
	SessionRejectedByUserEvent                           = "DOMAINEVENT#SessionRejectedByUser"
	SessionRejectedOnMatchingTimeoutEvent                = "DOMAINEVENT#SessionRejectedOnMatchingTimeout"
	SessionRejectedOnNoTutorsEvent                       = "DOMAINEVENT#SessionRejectedOnNoTutors"
	SessionClosedByUserEvent                             = "DOMAINEVENT#SessionClosedByUser"
	SessionClosedByTutorEvent                            = "DOMAINEVENT#SessionClosedByTutor"
	SessionClosedOnTutorDisconnectedEvent                = "DOMAINEVENT#SessionClosedOnTutorDisconnected"
	SessionRatedByUserEvent                              = "DOMAINEVENT#SessionRatedByUser"
	SessionReportedByTutorEvent                          = "DOMAINEVENT#SessionReportedByTutor"
	QuestionUpdatedEvent                                 = "DOMAINEVENT#QuestionUpdated"
	TutorUnassignedFromSessionOnConfirmationTimeoutEvent = "DOMAINEVENT#TutorUnassignedFromSessionOnConfirmationTimeout"
	TutorUnassignedFromSessionOnTutorDisconnectedEvent   = "DOMAINEVENT#TutorUnassignedFromSessionOnTutorDisconnected"
	TutorAssignedToSessionEvent                          = "DOMAINEVENT#TutorAssignedToSession"
)

func fillStatBasedOnItem(stats *SessionStats, item DynamoItem) {
	switch {
	case strings.HasPrefix(item.Metadata, SessionMetadata):
		stats.Market = item.Market
	case strings.HasPrefix(item.Metadata, SessionCreatedByUserEvent):
		stats.CreatedAt = item.CreatedAt
		stats.CreatedByRole = "USER"
	case strings.HasPrefix(item.Metadata, SessionCreatedByTutorEvent):
		stats.CreatedAt = item.CreatedAt
		stats.CreatedByRole = "TUTOR"
	case strings.HasPrefix(item.Metadata, SessionConfirmedByTutorEvent):
		stats.ConfirmedAt = item.CreatedAt
	case strings.HasPrefix(item.Metadata, SessionRejectedByUserEvent):
		stats.RejectedAt = item.CreatedAt
		stats.RejectedReason = "user"
	case strings.HasPrefix(item.Metadata, SessionRejectedOnMatchingTimeoutEvent):
		stats.RejectedAt = item.CreatedAt
		stats.RejectedReason = "matching_timeout"
	case strings.HasPrefix(item.Metadata, SessionRejectedOnNoTutorsEvent):
		stats.RejectedAt = item.CreatedAt
		stats.RejectedReason = "no_tutors"
	case strings.HasPrefix(item.Metadata, SessionClosedByTutorEvent):
		stats.ClosedAt = item.CreatedAt
		stats.ClosedReason = "tutor"
	case strings.HasPrefix(item.Metadata, SessionClosedByUserEvent):
		stats.ClosedAt = item.CreatedAt
		stats.ClosedReason = "user"
	case strings.HasPrefix(item.Metadata, SessionClosedOnTutorDisconnectedEvent):
		stats.ClosedAt = item.CreatedAt
		stats.ClosedReason = "tutor_disconnected"
	case strings.HasPrefix(item.Metadata, TutorAssignedToSessionEvent):
		stats.NoOfAssignAttempts += 1
	case strings.HasPrefix(item.Metadata, SessionRatedByUserEvent):
	case strings.HasPrefix(item.Metadata, SessionReportedByTutorEvent):
	case strings.HasPrefix(item.Metadata, QuestionUpdatedEvent):
	case strings.HasPrefix(item.Metadata, TutorUnassignedFromSessionOnConfirmationTimeoutEvent):
	case strings.HasPrefix(item.Metadata, TutorUnassignedFromSessionOnTutorDisconnectedEvent):
	default:
		panic("Unknown item")
	}
}

func marshalToCSV(statsMap map[string]*SessionStats) string {
	stats := make([]*SessionStats, 0, len(statsMap))

	for _, v := range statsMap {
		stats = append(stats, v)
	}

	csvContent, err := gocsv.MarshalString(stats) // Get all clients as CSV string
	if err != nil {
		panic(err)
	}

	return csvContent
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), func(o *config.LoadOptions) error {
		o.Region = "eu-west-1"
		return nil
	})
	if err != nil {
		panic(err)
	}

	svc := dynamodb.NewFromConfig(cfg)
	p := dynamodb.NewScanPaginator(svc, &dynamodb.ScanInput{
		TableName:        aws.String("session"),
		FilterExpression: aws.String("#createdAt > :createdAtFrom AND #createdAt < :createdAtTo AND (#metadata = :sessMeta OR begins_with(#metadata, :domainEventMeta))"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":createdAtFrom":   &types.AttributeValueMemberS{Value: "2022-03-01T00:00:00Z"},
			":createdAtTo":     &types.AttributeValueMemberS{Value: "2022-04-01T00:00:00Z"},
			":sessMeta":        &types.AttributeValueMemberS{Value: "SESSION"},
			":domainEventMeta": &types.AttributeValueMemberS{Value: "DOMAINEVENT#"},
		},
		ExpressionAttributeNames: map[string]string{
			"#createdAt": "createdAt",
			"#metadata":  "metadata",
		},
		ProjectionExpression: aws.String("id,metadata,createdAt,market"),
	})

	stats := make(map[string]*SessionStats)

	for p.HasMorePages() {
		out, err := p.NextPage(context.TODO())
		if err != nil {
			panic(err)
		}

		var pItems []DynamoItem
		err = attributevalue.UnmarshalListOfMaps(out.Items, &pItems)
		if err != nil {
			panic(err)
		}

		for _, item := range pItems {
			_, ok := stats[item.ID]
			if !ok {
				stats[item.ID] = &SessionStats{ID: item.ID}
			}

			fillStatBasedOnItem(stats[item.ID], item)
		}
	}

	fmt.Println(marshalToCSV(stats))
}
