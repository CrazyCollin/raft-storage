package common

const INIT_LOG_INDEX = 0

const SLOT_NUM = 10

var BUCKET_META_PREFIX = []byte{0x01, 0x09, 0x09, 0x08}

var RAFTLOG_PREFIX = []byte{0x11, 0x11, 0x19, 0x96}

var RAFT_STATE_KEY = []byte{0x19, 0x49}

var RAFT_SNAPSHOT_KEY = []byte{0x19, 0x97}
