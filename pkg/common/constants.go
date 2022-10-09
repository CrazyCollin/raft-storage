package common

const INIT_LOG_INDEX = 0

const SLOT_NUM = 10

const FILE_BLOCK_SIZE = 1024 * 1024 * 1

const FILE_BLOCK_SIZE_2MB = 1024 * 1024 * 2

const FILE_BLOCK_SIZE_4MB = 1024 * 1024 * 4

var BUCKET_META_PREFIX = []byte{0x01, 0x09, 0x09, 0x08}

var OBJECT_META_PREFIX = []byte{0x01, 0x09, 0x09, 0x05}

var CONFIG_META_PREFIX = []byte{0x01, 0x09, 0x09, 0x07}

var CURR_CONFIG_VERSION_KEY = []byte{0x01, 0x09, 0x09, 0x06}

var RAFTLOG_PREFIX = []byte{0x11, 0x11, 0x19, 0x96}

var RAFT_STATE_KEY = []byte{0x19, 0x49}

var RAFT_SNAPSHOT_KEY = []byte{0x19, 0x97}
