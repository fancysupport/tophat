package tophat

import (
	"bytes"
	"encoding/binary"
)

var AggregateHash = `
-- expects 1 key and 3 args: hash_key, expire_time, value
-- pack sum,min,max as doubles for precision, floats lose precision much too quickly
-- pack count as 32bit unsigned int (135/s for a year timestep)

-- convert arg to number
local new_val = 0 + ARGV[3]

-- cache lookups as locals
local rcall = redis.call
local key = KEYS[1]
local hash_key = ARGV[1]
local ttl = ARGV[2]

-- check key exists so we know if we have to set an expires
if rcall('exists', key) == 1 then
	local data = rcall('hget', key, hash_key)

	if data then
		local count, sum, min, max = struct.unpack('<Iddd', data)

		sum = sum + new_val

		-- if are way faster than math.min
		if min > new_val then min = new_val end
		if max < new_val then max = new_val end

		data = struct.pack('<Iddd', count+1, sum, min, max)
		rcall('hset', key, hash_key, data)
	else
		data = struct.pack('<Iddd', 1, new_val, new_val, new_val)
		rcall('hset', key, hash_key, data)
	end

else
	local data = struct.pack('<Iddd', 1, new_val, new_val, new_val)
	rcall('hset', key, hash_key, data)
	rcall('expireat', key, ttl)
end

return 1
`

type AggregateHashData struct {
	Count uint32
	Sum   float64
	Min   float64
	Max   float64
}

func AggregateHashUnpack(data []byte) (AggregateHashData, error) {
	var header AggregateHashData
	err := binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &header)
	return header, err
}

func AggregateHashPick(data AggregateHashData, fn MetricFn) float64 {
	switch fn {
	case CountFn:
		return float64(data.Count)
	case SumFn:
		return data.Sum
	case MinFn:
		return data.Min
	case MaxFn:
		return data.Max
	case AvgFn:
		return data.Sum / float64(data.Count)
	}

	return -1
}
