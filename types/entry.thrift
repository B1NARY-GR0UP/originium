namespace go types

struct Entry {
    1: string key,
    2: binary value,
    3: bool tombstone
    4: i64 version
    // TODO: expires_at
}
