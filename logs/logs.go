package logs

import (
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
)

var baseLogger = logging.Logger("saturn-node")

type SaturnLogger struct {
	logger    *logging.ZapEventLogger
	subsystem string
}

func NewSaturnLogger() *SaturnLogger {
	return &SaturnLogger{
		logger: baseLogger,
	}
}

func (s *SaturnLogger) Subsystem(name string) *SaturnLogger {
	return &SaturnLogger{
		logger:    logging.Logger(s.subsystem + name),
		subsystem: name,
	}
}

func (s *SaturnLogger) Debugw(reqID uuid.UUID, msg string, kvs ...interface{}) {
	kvs = paramsWithReqID(reqID, kvs...)
	s.logger.Debugw(msg, kvs...)
}

func (s *SaturnLogger) Infow(reqID uuid.UUID, msg string, kvs ...interface{}) {
	kvs = paramsWithReqID(reqID, kvs...)
	s.logger.Infow(msg, kvs...)
}

func (s *SaturnLogger) Warnw(reqID uuid.UUID, msg string, kvs ...interface{}) {
	kvs = paramsWithReqID(reqID, kvs...)
	s.logger.Warnw(msg, kvs...)
}

func (s *SaturnLogger) Errorw(reqID uuid.UUID, errMsg string, kvs ...interface{}) {
	kvs = paramsWithReqID(reqID, kvs...)

	s.logger.Errorw(errMsg, kvs...)
}

func (s *SaturnLogger) LogError(reqID uuid.UUID, errMsg string, err error) {
	s.Errorw(reqID, errMsg, "err", err.Error())
}

func paramsWithReqID(reqID uuid.UUID, kvs ...interface{}) []interface{} {
	kvs = append([]interface{}{"id", reqID}, kvs...)
	return kvs
}
