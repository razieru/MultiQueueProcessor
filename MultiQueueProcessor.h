#pragma once

#include <unordered_map>
#include <list>
#include <thread>
#include <utility>
#include <mutex>
#include <atomic>
#include <functional>

namespace mqp {
	template<typename Key, typename Value>
	class IConsumer {
	public:
		virtual ~IConsumer() = default;
		virtual void Consume(const Key id, const Value& value) noexcept = 0;
	};

	template<typename Key, typename Value>
	class MultiQueueProcessor {
	public:
		MultiQueueProcessor(const std::size_t maxChannelSize = 1000) :
			maxChannelSize_(maxChannelSize),
			running_{ true },
			th_(std::bind(&MultiQueueProcessor::Process, this)) {}

		~MultiQueueProcessor() {
			running_ = false;
			th_.join();
		}
		MultiQueueProcessor(const MultiQueueProcessor&) = delete;
		MultiQueueProcessor(MultiQueueProcessor&&) = delete;
		MultiQueueProcessor& operator=(const MultiQueueProcessor&) = delete;
		MultiQueueProcessor& operator=(MultiQueueProcessor&&) = delete;

		using IConsumer_ptr = std::shared_ptr<IConsumer<Key, Value>>;

		void Subscribe(const Key id, const IConsumer_ptr& consumer) {
			std::lock_guard<std::mutex> lock{ consumerMutex_ };
			consumers_.insert(std::make_pair(id, consumer));
		}

		void Unsubscribe(const Key id) {
			std::lock_guard<std::mutex> lock{ consumerMutex_ };
			consumers_.erase(id);
		}

		void Enqueue(const Key id, const Value& value) {
			std::lock_guard<std::mutex> lock{ channelMutex_ };
			const auto channelIter = channels_.find(id);
			if (channelIter != channels_.end()) {
				if (channelIter->second.size() >= maxChannelSize_)
					channelIter->second.pop_front();
				channelIter->second.push_back(value);
			} else
				channels_.insert(std::make_pair(id, std::list<Value>{value}));
		}


	private:
		void Process() {
			while (running_) {
				std::this_thread::yield();
				std::lock_guard<std::mutex> lock{ consumerMutex_ };
				for (const auto& consumer : consumers_) {
					std::lock_guard<std::mutex> lock{ channelMutex_ };
					const auto channelIter = channels_.find(consumer.first);
					if ((channelIter != channels_.end()) &&
							!channelIter->second.empty()) {
						const auto value = channelIter->second.front();
						channelIter->second.pop_front();
						consumer.second->Consume(consumer.first, value);
					}
				}
			}
		}

		const std::size_t maxChannelSize_;

		std::unordered_map<Key, IConsumer_ptr> consumers_;
		std::unordered_map<Key, std::list<Value>> channels_;

		std::atomic<bool> running_;
		std::mutex consumerMutex_;
		std::mutex channelMutex_;
		std::thread th_;
	};
}
