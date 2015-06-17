class SSDB
  T_BOOL    = lambda { |r| r == "1" }
  T_INT     = lambda{| r | r.to_i }
  T_CINT    = lambda { |r| r.to_i if r }
  T_VBOOL   = lambda {|r| r.each_slice(2).map {|_, v| v == "1" }}
  T_VINT    = lambda {|r| r.each_slice(2).map {|_, v| v.to_i }}
  T_STRSTR  = lambda {|r|r.each_slice(2).to_a }
  T_ARRAY  = lambda {|r|r.each_slice(2).map {|v, s| v } }
  T_STRINT  = lambda {|r| r.each_slice(2).map {|v, s| [v, s.to_i] } }
  T_MAPINT  = lambda do |r, n| 
      h = {}; r.each_slice(2) { |k, v| h[k] = v }; n.map {|k| h[k].to_i }
   end
  T_MAPSTR  = lambda do |r, n| 
    h = {}; r.each_slice(2) {|k, v| h[k] = v }; n.map {|k| h[k] } 
  end
  T_HASHSTR = lambda do | r| 
   h = {}; r.each_slice(2) {|k, v| h[k] = v }; h 
 end
  T_HASHINT = lambda do|r| 
    h = {}; r.each_slice(2) {|k, v| h[k] = v.to_i }; h 
 end
  BLANK     = "".freeze

  DB_STATS  = ["compactions", "level", "size", "time", "read", "written"].freeze
  T_INFO    = lambda do  |rows| 
    res = {}
    rows.shift # skip first
    rows.each_slice(2) do |key, val|
      res[key] = case key
      when "leveldb.stats"
        stats = {}
        val.lines.to_a.last.strip.split(/\s+/).each_with_index do |v, i|
          stats[DB_STATS[i]] = v.to_i
        end
        stats
      when /^cmd\./
        val.split("\t").inject({}) do |stats, i|
          k, v = i.split(": ", 2)
          stats.update k => v.to_i
        end
      else
        val.to_i
      end
    end
    res
  end
end