package journal

import "github.com/twmb/franz-go/pkg/kgo"

// journalAwarePartitioner routes Counter's transactional producer records
// to two different partitioning strategies depending on topic:
//
//   - counter-journal: ManualPartitioner — the caller fills
//     Record.Partition = shard.Index(user_id, VShardCount) so that
//     1 vshard ↔ 1 partition (ADR-0058 §2a).
//   - everything else (per-symbol order-event, ADR-0050):
//     StickyKeyPartitioner — hash by record key (symbol) so all events
//     for one symbol land on one partition.
//
// franz-go only accepts one Partitioner per client, so we wrap both and
// dispatch at ForTopic time.
type journalAwarePartitioner struct {
	journalTopic string
	manual       kgo.Partitioner
	sticky       kgo.Partitioner
}

func newJournalAwarePartitioner(journalTopic string) kgo.Partitioner {
	return &journalAwarePartitioner{
		journalTopic: journalTopic,
		manual:       kgo.ManualPartitioner(),
		sticky:       kgo.StickyKeyPartitioner(nil),
	}
}

// ForTopic implements kgo.Partitioner.
func (p *journalAwarePartitioner) ForTopic(topic string) kgo.TopicPartitioner {
	if topic == p.journalTopic {
		return p.manual.ForTopic(topic)
	}
	return p.sticky.ForTopic(topic)
}
