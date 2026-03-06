<?php

/**
 * IDE stub for cache backend (e.g. Fat-Free Cache or pathfinder bootstrap).
 * Used by MapUpdate for dmchelper_min_version. Not loaded at runtime.
 *
 * @see pathfinder: \Cache::instance() from F3 when run in pathfinder context
 */
class Cache
{
    /** @return self */
    public static function instance(): self
    {
        return new self();
    }

    /** @param string $key @return mixed */
    public function get($key)
    {
        return null;
    }

    /** @param string $key @param mixed $val @param int $ttl @return mixed */
    public function set($key, $val, $ttl = 0)
    {
        return null;
    }
}
