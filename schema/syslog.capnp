
@0xd783e67fdcef1e18;

struct SyslogMessage {
  timestamp     @0 :UInt64;
  source        @1 :Text;
  facility      @2 :UInt8;
  severity      @3 :UInt8;
  rawMessage    @4 :Text;
}