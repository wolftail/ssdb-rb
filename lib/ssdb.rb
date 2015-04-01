require "monitor"

class SSDB
  include MonitorMixin

  # @attr_reader [SSDB::Client] the client
  attr_reader :client

  # @return [SSDB] the current/global SSDB connection
  def self.current
    @current ||= SSDB.new
  end

  # @param [SSDB] ssdb the current/global SSDB connection
  def self.current=(ssdb)
    @current = ssdb
  end

  # @see SSDB::Client#initialize
  def initialize(*a)
    @client = Client.new(*a)
    super() # Monitor#initialize
  end

  # Execute a batch operation
  #
  # @example Simple batch
  #
  #   ssdb.batch do
  #     ssdb.set "foo", "5"
  #     ssdb.get "foo"
  #     ssdb.incr "foo"
  #   end
  #   # => [true, "5", 6]
  #
  # @example Using futures
  #
  #   ssdb.batch do
  #     v = ssdb.set "foo", "5"
  #     w = ssdb.incr "foo"
  #   end
  #
  #   v.value
  #   # => true
  #   w.value
  #   # => 6
  #
  def batch
    mon_synchronize do
      begin
        original, @client = @client, SSDB::Batch.new
        yield(self)
        @client.values = original.perform(@client)
      ensure
        @client = original
      end
    end
  end

  # Returns info
  # @return [Hash] info attributes
  def info
    mon_synchronize do
      perform ["info"], :proc =>  T_INFO
    end
  end


  # Evaluates scripts
  #
  # @param [String] script
  # @param [multiple<String>] args
  # @return [Array] results
  #
  # @example
  #   script =<<-LUA
  #     local x = math.pi * 10
  #     return x
  #   LUA
  #   ssdb.eval(script)
  #   # => ["31.425926"]
  #
  def eval(script, *args)
    mon_synchronize do
      perform ["eval", script, *args]
    end
  end

  # Returns value at `key`.
  #
  # @param [String] key the key
  # @return [String] the value
  #
  # @example
  #   ssdb.get("foo") # => "val"
  def get(key)
    mon_synchronize do
      perform ["get", key]
    end
  end

  def getset(key, value)
    mon_synchronize do
      perform ["getset", key, value]
    end
  end

  def getbit(key, offset)
    mon_synchronize do
      perform ["getbit", key, offset], :proc =>  T_INT
    end
  end

  def setbit(key, offset, value)
    mon_synchronize do
      perform ['setbit', key, offset, value]
    end
  end

  def bitcount(key, start = 0, size = nil)
    mon_synchronize do
      if size.nil?
        perform ["countbit", key, start], :proc =>  T_INT
      else
        perform ["countbit", key, start, size], :proc =>  T_INT
      end
    end
  end


  # Sets `value` at `key`.
  #
  # @param [String] key the key
  # @param [String] value the value
  #
  # @example
  #   ssdb.set("foo", "val") # => true
  def set(key, value)
    mon_synchronize do
      perform ["set", key, value], :proc =>  T_BOOL
    end
  end

  def setex(key, ttl, value)
    mon_synchronize do
      perform ['setx', key, value, ttl], :proc =>  T_BOOL
    end
  end

  def setnx(key, value)
    mon_synchronize do
      perform ['setnx', key, value], :proc =>  T_BOOL
    end
  end


  def expire(key, ttl)
    mon_synchronize do
      perform ["expire", key, ttl], :proc =>  T_BOOL
    end
  end

  def ttl(key)
    mon_synchronize do
      perform ["ttl", key], :proc =>  T_INT
    end
  end


  # Increments a `key` by value
  #
  # @param [String] key the key
  # @param [Integer] value the increment
  #
  # @example
  #   ssdb.incr("foo") # => 1
  def incr(key, value = 1)
    mon_synchronize do
      perform ["incr", key, value], :proc =>  T_INT
    end
  end

  def incrby(key, value)
    mon_synchronize do
      perform ["incr", key, value], :proc =>  T_INT
    end
  end

  # Decrements a `key` by value
  #
  # @param [String] key the key
  # @param [Integer] value the decrement
  #
  # @example
  #   ssdb.decr("foo") # => -1
  def decr(key, value = 1)
    mon_synchronize do
      perform ["decr", key, value], :proc =>  T_INT
    end
  end

  def decrby(key, value = 1)
    mon_synchronize do
      perform ["decr", key, value], :proc =>  T_INT
    end
  end

  # Checks existence of `key`.
  #
  # @param [String] key the key
  # @return [Boolean] true if exists
  #
  # @example
  #   ssdb.exists("foo") # => true
  def exists(key)
    mon_synchronize do
      perform ["exists", key], :proc =>  T_BOOL
    end
  end

  alias_method :exists?, :exists

  # Delete `key`.
  #
  # @param [String] key the key
  #
  # @example
  #   ssdb.del("foo") # => nil
  def del(key)
    mon_synchronize do
      perform ["del", key]
    end
  end

  # Scans keys between `start` and `stop`.
  #
  # @param [String] start start at this key
  # @param [String] stop stop at this key
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<String>] matching keys
  #
  # @example
  #   ssdb.keys("a", "z", limit: 2) # => ["bar", "foo"]
  def keys(start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["keys", start, stop, limit], :multi =>  true
    end
  end

  # Scans keys between `start` and `stop`.
  #
  # @param [String] start start at this key
  # @param [String] stop stop at this key
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<Array<String,String>>] key/value pairs
  #
  # @example
  #   ssdb.scan("a", "z", limit: 2)
  #   # => [["bar", "val1"], ["foo", "val2"]]
  def scan(start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["scan", start, stop, limit], :multi =>  true, :proc =>  T_STRSTR
    end
  end

  # Reverse-scans keys between `start` and `stop`.
  #
  # @param [String] start start at this key
  # @param [String] stop stop at this key
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<Array<String,String>>] key/value pairs in reverse order
  #
  # @example
  #   ssdb.rscan("z", "a", limit: 2)
  #   # => [["foo", "val2"], ["bar", "val1"]]
  def rscan(start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["rscan", start, stop, limit], :multi =>  true, :proc =>  T_STRSTR
    end
  end

  # Sets multiple keys
  #
  # @param [Hash] pairs key/value pairs
  #
  # @example
  #   ssdb.multi_set("bar" => "val1", "foo" => "val2")
  #   # => 4
  def multi_set(pairs)
    mon_synchronize do
      perform ["multi_set", *pairs.to_a].flatten, :proc =>  T_INT
    end
  end

  # Retrieves multiple keys
  #
  # @param [Array<String>] keys
  # @return [Array<String>] values
  #
  # @example
  #   ssdb.multi_get(["bar", "foo"])
  #   # => ["val1", "val2"]
  def multi_get(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_get", *keys], :multi =>  true, :proc =>  T_MAPSTR, args: [keys]
    end
  end

  def mget(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_get", *keys], :multi =>  true, :proc =>  T_MAPSTR, args: [keys]
    end
  end

  # Retrieves multiple keys
  #
  # @param [Array<String>] keys
  # @return [Array<String>] values
  #
  # @example
  #   ssdb.mapped_multi_get(["bar", "foo"])
  #   # => {"bar" => "val1", "foo" => val2"}
  def mapped_multi_get(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_get", *keys], :multi =>  true, :proc =>  T_HASHSTR
    end
  end

  # Deletes multiple keys
  #
  # @param [Array<String>] keys
  #
  # @example
  #   ssdb.multi_del(["bar", "foo"])
  #   # => 2
  def multi_del(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_del", *keys], :proc =>  T_INT
    end
  end

  # Checks existence of multiple keys
  #
  # @param [Array<String>] keys
  # @return [Array<Boolean>] results
  #
  # @example
  #   ssdb.multi_exists(["bar", "foo", "baz"])
  #   # => [true, true, false]
  def multi_exists(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_exists", *keys], :multi =>  true, :proc =>  T_VBOOL
    end
  end

  alias_method :multi_exists?, :multi_exists

  # Returns the score of `member` at `key`.
  #
  # @param [String] key the key
  # @param [String] member the member
  # @return [Integer] the score
  #
  # @example
  #   ssdb.zget("visits", "u1")
  #   # => 101
  def zget(key, member)
    mon_synchronize do
      perform ["zget", key, member], :proc =>  T_CINT
    end
  end

  def zscore(key, member)
    mon_synchronize do
      perform ["zget", key, member], :proc =>  T_CINT
    end
  end

  # Sets the `score` of `member` at `key`.
  #
  # @param [String] key the key
  # @param [String] member the member
  # @param [Integer] score the score
  #
  # @example
  #   ssdb.zset("visits", "u1", 202)
  #   # => true
  def zset(key, member, score)
    mon_synchronize do
      perform ["zset", key, member, score], :proc =>  T_BOOL
    end
  end


  # Redis 'compatibility'.
  #
  # @param [String] key the key
  # @param [Integer] score the score
  # @param [String] member the member
  #
  # @example
  #   ssdb.zadd("visits", 202, "u1")
  #   # => true
  def zadd(key, score, member)
    zset(key, member, score)
  end

  # Increments the `member` in `key` by `score`
  #
  # @param [String] key the key
  # @param [String] member the member
  # @param [Integer] score the increment
  #
  # @example
  #   ssdb.zincr("visits", "u1")
  #   # => 102
  #   ssdb.zincr("visits", "u1", 100)
  #   # => 202
  def zincr(key, member, score = 1)
    mon_synchronize do
      perform ["zincr", key, member, score], :proc =>  T_INT
    end
  end

  # Decrements the `member` in `key` by `score`
  #
  # @param [String] key the key
  # @param [String] member the member
  # @param [Integer] score the decrement
  #
  # @example
  #   ssdb.zdecr("visits", "u1")
  #   # => 100
  #   ssdb.zdecr("visits", "u1", 5)
  #   # => 95
  def zdecr(key, member, score = 1)
    mon_synchronize do
      perform ["zdecr", key, member, score], :proc =>  T_INT
    end
  end

  # Checks existence of a zset at `key`.
  #
  # @param [String] key the key
  # @return [Boolean] true if exists
  #
  # @example
  #   ssdb.zexists("visits")
  #   # => true
  def zexists(key)
    mon_synchronize do
      perform ["zexists", key], :proc =>  T_BOOL
    end
  end

  alias_method :zexists?, :zexists

  # Returns the cardinality of a set `key`.
  #
  # @param [String] key the key
  #
  # @example
  #   ssdb.zsize("visits")
  #   # => 2

  def zcard(key)
    mon_synchronize do
      perform ["zsize", key], :proc =>  T_INT
    end
  end

  def zsize(key)
    mon_synchronize do
      perform ["zsize", key], :proc =>  T_INT
    end
  end

  def zrem(key, member)
    mon_synchronize do
      perform ["zdel", key, member], :proc =>  T_BOOL
    end
  end

  # Delete an `member` from a zset `key`.
  #
  # @param [String] key the key
  # @param [String] member the member
  #
  # @example
  #   ssdb.zdel("visits", "u1")
  #   # => true
  def zdel(key, member)
    mon_synchronize do
      perform ["zdel", key, member], :proc =>  T_BOOL
    end
  end

  # Delete All data from key
  def clear(key)

    mon_synchronize do
      perform(["hclear", key], :proc =>  T_INT) +
          perform(["zclear", key], :proc =>  T_INT) +
          perform(["qclear", key], :proc =>  T_INT) +
          perform(["del", key], :proc =>  T_INT)
    end
  end

  def zclear(key)
    mon_synchronize do
      perform ["zclear", key], :proc =>  T_INT
    end
  end

  def zrange(key, start, stop, opts={})
    total = zcard(key)
    if total < 1
      return []
    end
    if start < 0
      start = total + start
    end
    if stop < 0
      stop = total + stop
    end

    limit = stop - start + 1
    mon_synchronize do
      if opts[:withscores]
        perform ["zrange", key, start, limit], :multi =>  true, :proc =>  T_STRINT
      else
        perform ["zrange", key, start, limit], :multi =>  true, :proc =>  T_ARRAY
      end
    end
  end

  def zrevrange(key, start, stop, opts={})
    total = zcard(key)
    if total < 1
      return []
    end
    if start < 0
      start = total + start
    end
    if stop < 0
      stop = total + stop
    end

    limit = stop - start + 1
    mon_synchronize do
      if opts[:withscores]
        perform ["zrrange", key, start, limit], :multi =>  true, :proc =>  T_STRINT
      else
        perform ["zrrange", key, start, limit], :multi =>  true, :proc =>  T_ARRAY
      end
    end
  end

  def qclear(key)
    mon_synchronize do
      perform ["qclear", key], :proc =>  T_INT
    end
  end

  def llen(key)
    mon_synchronize do
      perform ["qsize", key], :proc =>  T_INT
    end
  end

  def lpush(key, value)
    mon_synchronize do
      perform ["qpush_front", key, value]
    end
  end

  def rpush(key, value)
    mon_synchronize do
      perform ["qpush_back", key, value]
    end
  end

  def lpop(key)
    mon_synchronize do
      perform ["qpop_front", key]
    end
  end

  def rpop(key)
    mon_synchronize do
      perform ["qpop_back", key]
    end
  end

  def lrange(key, start, stop)

    mon_synchronize do
      perform ["qslice", key, start, stop], :multi =>  true
    end
  end

  def lindex(key, index)
    mon_synchronize do
      perform ["qget", key, index]
    end
  end

  def lset key, index, value
    mon_synchronize do
      perform ["qset", key, index, value]
    end
  end

  def qlist name_start, name_end, limit
    mon_synchronize do
      perform ["qlist", name_start, name_end, limit], :proc =>  T_STRSTR
    end
  end

  def hdel(key, member)
    mon_synchronize do
      perform ["hdel", key, member], :proc =>  T_BOOL
    end
  end

  def hget(key, member)
    mon_synchronize do
      perform ["hget", key, member]
    end
  end

  def pipelined
    mon_synchronize do
      begin
        original, @client = @client, SSDB::Batch.new
        yield(self)
        @client.values = original.perform(@client)
      ensure
        @client = original
      end
    end
  end

  def hmget(key, members)
    members = Array(members) unless members.is_a?(Array)
    return {} if members.size < 1
    members = members.map { |x| x.to_s }
    mon_synchronize do
      perform ["multi_hget", key, *members], :multi =>  true, :proc =>  T_MAPSTR, args: [members]
    end
  end

  def hclear(key)
    mon_synchronize do
      perform ["hclear", key], :proc =>  T_INT
    end
  end

  def hlen(key)

    mon_synchronize do
      perform ["hsize", key], :proc =>  T_INT
    end
  end

  def hexists(key, member)
    mon_synchronize do
      perform ["hexists", key, member], :proc =>  T_BOOL
    end
  end

  def hset(key, member, value)
    mon_synchronize do
      perform ["hset", key, member, value], :proc =>  T_BOOL
    end
  end

  def hgetall(key)
    mon_synchronize do
      perform ["hgetall", key],:multi =>  true, :proc =>  T_HASHSTR
    end
  end

  # List zset keys between `start` and `stop`.
  #
  # @param [String] start start at this key
  # @param [String] stop stop at this key
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<String>] matching zset keys
  #
  # @example
  #   ssdb.zlist("a", "z", limit: 2)
  #   # => ["visits", "page_views"]
  def zlist(start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["zlist", start, stop, limit], :multi =>  true
    end
  end

  def zrlist(start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["zrlist", start, stop, limit], :multi =>  true
    end
  end

  def zcount(key, min, max)
    mon_synchronize do
      perform ["zcount", key, min, max], :proc =>  T_INT
    end
  end


  # Lists members at `key` starting at `start_member`
  # between `start` and `stop` scores.
  #
  # @param [String] key the zset
  # @param [Integer] start start at this score
  # @param [Integer] stop stop at this score
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<String>] matching members
  #
  # @example
  #   ssdb.zkeys("visits", 0, 300, limit: 2)
  #   # => ["u1", "u2"]
  def zkeys(key, start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["zkeys", key, BLANK, start, stop, limit], :multi =>  true
    end
  end

  # Scans for members at `key` starting at `start_member`
  # between `start` and `stop` scores.
  #
  # @param [String] key the zset
  # @param [Integer] start start at this score
  # @param [Integer] stop stop at this score
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<Array<String,Integer>>] member/score pairs
  #
  # @example
  #   ssdb.zscan("visits", 0, 300, limit: 2)
  #   # => [["u1", 101], ["u2", 202]]
  def zscan(key, start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["zscan", key, BLANK, start, stop, limit], :multi =>  true, :proc =>  T_STRINT
    end
  end

  def zrevrangebyscore key, max, min, opts={}
    limit = opts[:limit] || [0, -1]

    offset = limit[0]
    count = limit[1]

    per_page = count
    if per_page <= 0
      per_page = 1000
    end

    result = []
    mon_synchronize do
      score_start = max
      key_start = ''
      read_num = 0
      loop do

        page_result = perform ["zrscan", key, key_start, score_start, min, per_page], :multi =>  true, :proc =>  T_STRINT

        if page_result.size == 0
          break
        end
        key_start = page_result[-1][0]
        score_start = page_result[-1][1]

        read_num += page_result.size
        if read_num < offset
          next
        end


        page_result.each_with_index do |item, i|
          next if read_num - page_result.size + i < offset
          if opts[:withscores]
            result << item
          else
            result << item[0]
          end

          if result.size > count && count > 0
            break
          end
        end


        if result.size >= count && count > 0
          break
        end
      end
    end
    result[0, count]
  end

  def zremrangebyrank(key, start, stop)
    mon_synchronize do
      perform ["zremrangebyrank", key, start, stop], :proc =>  T_INT
    end
  end

  def zremrangebyscore(key, min, max)
    mon_synchronize do
      perform ["zremrangebyscore", key, min, max], :proc =>  T_INT
    end
  end

  def zrangebyscore(key, min, max, opts = {})

    limit = opts[:limit] || [0, -1]

    offset = limit[0]
    count = limit[1]

    per_page = count
    if per_page <= 0
      per_page = 1000
    end

    result = []
    mon_synchronize do
      score_start = min
      key_start = ''
      read_num = 0
      loop do

        page_result = perform ["zscan", key, key_start, score_start, max, per_page], :multi =>  true, :proc =>  T_STRINT

        if page_result.size == 0
          break
        end
        key_start = page_result[-1][0]
        score_start = page_result[-1][1]

        read_num += page_result.size
        if read_num < offset
          next
        end


        page_result.each_with_index do |item, i|
          next if read_num - page_result.size + i < offset
          if opts[:withscores]
            result << item
          else
            result << item[0]
          end

          if result.size > count && count > 0
            break
          end
        end


        if result.size >= count && count > 0
          break
        end
      end
    end
    result[0, count]
  end

  # Reverse scans for members at `key` starting at `start_member`
  # between `start` and `stop` scores.
  #
  # @param [String] key the zset
  # @param [Integer] start start at this score
  # @param [Integer] stop stop at this score
  # @param [Hash] opts options
  # @option opts [Integer] :limit limit results
  # @return [Array<Array<String,Integer>>] member/score pairs
  #
  # @example
  #   ssdb.zrscan("visits", 300, 0, limit: 2)
  #   # => [["u2", 202], ["u1", 101]]
  def zrscan(key, start, stop, opts = {})
    limit = opts[:limit] || -1
    mon_synchronize do
      perform ["zrscan", key, BLANK, start, stop, limit], :multi =>  true, :proc =>  T_STRINT
    end
  end

  # Checks existence of multiple sets
  #
  # @param [Array<String>] keys
  # @return [Array<Boolean>] results
  #
  # @example
  #   ssdb.multi_zexists("visits", "page_views", "baz")
  #   # => [true, true, false]
  def multi_zexists(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_zexists", *keys], :multi =>  true, :proc =>  T_VBOOL
    end
  end

  alias_method :multi_zexists?, :multi_zexists

  # Returns cardinalities of multiple sets
  #
  # @param [Array<String>] keys
  # @return [Array<Boolean>] results
  #
  # @example
  #   ssdb.multi_zsize("visits", "page_views", "baz")
  #   # => [2, 1, 0]
  def multi_zsize(keys)
    keys = Array(keys) unless keys.is_a?(Array)
    mon_synchronize do
      perform ["multi_zsize", *keys], :multi =>  true, :proc =>  T_VINT
    end
  end

  # Sets multiple members of `key`
  #
  # @param [String] key the zset
  # @param [Hash<String,Integer>] pairs key/value pairs
  #
  # @example
  #   ssdb.multi_zset("visits", "u1" => 102, "u3" => 303)
  #   # => 2
  def multi_zset(key, pairs)
    mon_synchronize do
      perform ["multi_zset", key, *pairs.to_a].flatten, :proc =>  T_INT
    end
  end

  # Retrieves multiple scores from `key`
  #
  # @param [String] key the zset
  # @param [Array<String>] members
  # @return [Array<Integer>] scores
  #
  # @example
  #   ssdb.multi_zget("visits", ["u1", "u2"])
  #   # => [101, 202]
  def multi_zget(key, members)
    members = Array(members) unless members.is_a?(Array)
    mon_synchronize do
      perform ["multi_zget", key, *members], :multi =>  true, :proc =>  T_MAPINT, args: [members]
    end
  end

  # Retrieves multiple scores from `key`
  #
  # @param [String] key the zset
  # @param [Array<String>] members
  # @return [Hash<String,Integer>] members with scores
  #
  # @example
  #   ssdb.mapped_multi_zget("visits", ["u1", "u2"])
  #   # => {"u1" => 101, "u2" => 202}
  def mapped_multi_zget(key, members)
    members = Array(members) unless members.is_a?(Array)

    mon_synchronize do
      perform ["multi_zget", key, *members], :multi =>  true, :proc =>  T_HASHINT
    end
  end

  # Deletes multiple members from `key`
  #
  # @param [String] key the zset
  # @param [Array<String>] members
  #
  # @example
  #   ssdb.multi_zdel("visits", ["u1", "u2"])
  #   # => 2
  def multi_zdel(key, members)
    members = Array(members) unless members.is_a?(Array)
    mon_synchronize do
      perform ["multi_zdel", key, *members], :proc =>  T_INT
    end
  end


  def perform(chain, opts = {})
    opts[:cmd] = chain.map(&:to_s)
    client.call(opts)
  end

end

%w|version errors constants client batch future|.each do |name|
  require "ssdb/#{name}"
end
