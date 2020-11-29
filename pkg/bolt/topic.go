package boltwatermill

import (
	"github.com/pkg/errors"
	"regexp"
)

var disallowedTopicCharacters = regexp.MustCompile(`[^A-Za-z0-9\-\$\:\.\_]`)

var ErrInvalidTopicName = errors.New("topic name should not contain characters matched by " + disallowedTopicCharacters.String())

func validateTopicName(topic string) error {
	if disallowedTopicCharacters.MatchString(topic) {
		return errors.Wrap(ErrInvalidTopicName, topic)
	}

	return nil
}
