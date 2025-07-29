class RedisCoordinator:
    def _register_scripts(self):
        """Registers Lua scripts for atomic operations."""
        finisher_script = """
        local key_to_incr = KEYS[1]
        local succeeded_key = KEYS[2]
        local failed_key = KEYS[3]
        local total_key = KEYS[4]
        local channel = ARGV[1]
        local workflow_id = ARGV[2]
        if redis.call('EXISTS', total_key) == 0 then return 0 end
        redis.call('INCR', key_to_incr)
        local succeeded = tonumber(redis.call('GET', succeeded_key))
        local failed = tonumber(redis.call('GET', failed_key))
        local total = tonumber(redis.call('GET', total_key))
        if total > 0 and (succeeded + failed) >= total then
            if redis.call('PUBLISH', channel, workflow_id) > 0 then return 1 end
        end
        return 0
        """
        self.finisher_script = self.redis.register_script(finisher_script)

        add_to_total_script = """
        local total_key = KEYS[1]
        local succeeded_key = KEYS[2]
        local failed_key = KEYS[3]
        local info_key = KEYS[4]

        local add = tonumber(ARGV[1])
        local ttl = tonumber(ARGV[2])
        local job_id = ARGV[3]

        redis.call('INCRBY', total_key, add)
        redis.call('SETNX', succeeded_key, 0)
        redis.call('SETNX', failed_key, 0)
        redis.call('SETNX', info_key, job_id)

        if redis.call('TTL', total_key) == -1 then redis.call('EXPIRE', total_key, ttl) end
        if redis.call('TTL', succeeded_key) == -1 then redis.call('EXPIRE', succeeded_key, ttl) end
        if redis.call('TTL', failed_key) == -1 then redis.call('EXPIRE', failed_key, ttl) end
        if redis.call('TTL', info_key) == -1 then redis.call('EXPIRE', info_key, ttl) end

        return 1
        """
        self.add_to_total_script = self.redis.register_script(add_to_total_script)

        decrement_total_script = """
        local total_key = KEYS[1]
        local subtract = tonumber(ARGV[1])
        if redis.call('EXISTS', total_key) == 1 then
            return redis.call('DECRBY', total_key, subtract)
        end
        return -1
        """
        self.decrement_total_script = self.redis.register_script(decrement_total_script)

    def add_to_total(self, workflow_id: str, count_to_add: int, job_id: str):
        """
        Atomically increments the total expected count for a workflow.
        """
        total_key = f"counter:{workflow_id}:total"
        succeeded_key = f"counter:{workflow_id}:succeeded"
        failed_key = f"counter:{workflow_id}:failed"
        info_key = f"workflow_info:{workflow_id}"

        self.add_to_total_script(
            keys=[total_key, succeeded_key, failed_key, info_key],
            args=[count_to_add, settings.REDIS_COUNTER_TTL_SECONDS, job_id]
        )

    def decrement_from_total(self, workflow_id: str, count_to_subtract: int):
        """
        Atomically decrements the total expected count for a workflow.
        """
        total_key = f"counter:{workflow_id}:total"
        return self.decrement_total_script(
            keys=[total_key],
            args=[count_to_subtract]
        )
