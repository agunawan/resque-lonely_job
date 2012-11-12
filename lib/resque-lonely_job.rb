require 'resque-lonely_job/version'

module Resque
  module Plugins
    module LonelyJob
      LOCK_TIMEOUT = 60 * 60 * 24 * 5 # 5 days

      def lock_timeout
        Time.now.utc + LOCK_TIMEOUT + 1
      end

      # Overwrite this method to uniquely identify which mutex should be used
      # for a resque worker.
      def redis_key(*args)
        "lonely_job:#{@queue}"
      end

      def can_lock_queue?(*args)
        Resque.redis.setnx(redis_key(*args), lock_timeout)
      end

      def unlock_queue(*args)
        Resque.redis.del(redis_key(*args))
      end

      def before_perform(*args)
        unless can_lock_queue?(*args)
          # can't get the lock, so re-enqueue the task
          recreate
          # and don't perform
          raise Resque::Job::DontPerform.new "Queue #{args[:queue]} already has a job running!"
        end
      end

      def around_perform(*args)
        begin
          yield
        ensure
          unlock_queue(*args)
        end
      end
    end
  end
end
