#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction *txn, const Rid &rid, int tab_fd)
{
    std::unique_lock<std::mutex> lock(latch_);
    LockDataId lock_id(tab_fd, rid);
    auto it = lock_table_.find(lock_id);

    if (it != lock_table_.end())
    {
        auto &queue = it->second.request_queue_;
        auto request_it = std::find_if(queue.begin(), queue.end(),
                                       [&txn](const LockRequest &req)
                                       { return req.txn_id_ == txn->txn_id; });

        if (request_it != queue.end())
        {
            // 如果事务已经在队列中，则直接返回（这里可能需要处理超时或等待逻辑）
            return false; // 假设不允许重复申请
        }

        // 检查当前锁模式是否允许加共享锁
        if (it->second.group_lock_mode_ == GroupLockMode::X)
        {
            // 如果已经是排他锁，则无法加共享锁
            LockRequest new_request(txn->txn_id, LockMode::SHARED);
            queue.push_back(new_request);
            // 需要实现等待逻辑（这里简化处理）
            return false;
        }

        // 否则，更新锁模式并授予锁
        it->second.group_lock_mode_ = update_group_lock_mode(queue.insert(queue.end(), {txn->txn_id, LockMode::SHARED}));
        it->second.request_queue_.back().granted_ = true;
    }
    else
    {
        // 如果锁不在表中，则直接创建新锁
        LockRequestQueue new_queue;
        new_queue.request_queue_.emplace_back(txn->txn_id, LockMode::SHARED);
        new_queue.group_lock_mode_ = LockMode::SHARED;
        lock_table_[lock_id] = new_queue;
    }

    // 锁已成功授予
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction *txn, const Rid &rid, int tab_fd)
{
    std::unique_lock<std::mutex> lock(latch_);
    LockDataId lock_id(tab_fd, rid);
    auto it = lock_table_.find(lock_id);

    if (it != lock_table_.end())
    {
        auto &queue = it->second.request_queue_;
        auto request_it = std::find_if(queue.begin(), queue.end(),
                                       [&txn](const LockRequest &req)
                                       { return req.txn_id_ == txn->txn_id; });

        if (request_it != queue.end())
        {
            // 如果事务已经在队列中，则直接返回（处理超时或等待逻辑）
            return false;
        }

        // 检查当前锁模式是否允许加排他锁
        if (it->second.group_lock_mode_ != GroupLockMode::NON_LOCK &&
            it->second.group_lock_mode_ != GroupLockMode::X)
        {
            // 如果已经有其他锁存在，则无法加排他锁（除非已经是X锁）
            LockRequest new_request(txn->txn_id, LockMode::EXLUCSIVE);
            queue.push_back(new_request);
            // 需要实现等待逻辑
            return false;
        }

        // 更新锁模式并授予锁
        it->second.group_lock_mode_ = GroupLockMode::X;
        queue.emplace_back(txn->txn_id, LockMode::EXLUCSIVE);
        queue.back().granted_ = true;
    }
    else
    {
        // 创建新锁
        LockRequestQueue new_queue;
        new_queue.request_queue_.emplace_back(txn->txn_id, LockMode::EXLUCSIVE);
        new_queue.group_lock_mode_ = GroupLockMode::X;
        lock_table_[lock_id] = new_queue;
    }

    return true;
}
/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction *txn, int tab_fd)
{

    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction *txn, int tab_fd)
{

    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction *txn, int tab_fd)
{

    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction *txn, int tab_fd)
{

    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction *txn, LockDataId lock_data_id)
{

    return true;
}