#include "lock_manager.h"
#include <shared_mutex>

/**
 * 申请行级读锁
 * @param txn 要申请锁的事务对象指针
 * @param rid 加锁的目标记录ID
 * @param tab_fd 记录所在的表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockSharedOnRecord(Transaction *txn, const Rid &rid, int tab_fd) {
    // Todo:
    // 1. 通过mutex申请访问全局锁表
    // 2. 检查事务的状态
    // 3. 查找当前事务是否已经申请了目标数据项上的锁，如果存在则根据锁类型进行操作，否则执行下一步操作
    // 4. 将要申请的锁放入到全局锁表中，并通过组模式来判断是否可以成功授予锁
    // 5. 如果成功，更新目标数据项在全局锁表中的信息，否则阻塞当前操作
    // 提示：步骤5中的阻塞操作可以通过条件变量来完成，
    //所有加锁操作都遵循上述步骤，在下面的加锁操作中不再进行注释提示
    if(txn->GetState()!=TransactionState::GROWING&&txn->GetState()!=TransactionState::DEFAULT){
        assert(0);
        return false;
    }
    txn->SetState(TransactionState::GROWING);
    auto lock_data_id=new LockDataId(tab_fd,rid,LockDataType::RECORD);
    std::unique_lock<std::mutex> lock(latch_);
    auto lock_set=txn->GetLockSet();

    for(auto lock:*lock_set){
        if(lock.fd_==tab_fd&&lock.rid_==rid){
            //一定会是读or写锁，所以不用判断直接return
            return true;
        }
    }

    while(lock_table_[*lock_data_id].group_lock_mode_!=GroupLockMode::S&&
        lock_table_[*lock_data_id].group_lock_mode_!=GroupLockMode::NON_LOCK
    ){
        lock_table_[*lock_data_id].cv_.wait(lock);
    }
    lock_table_[*lock_data_id].group_lock_mode_=GroupLockMode::S;
    lock_table_[*lock_data_id].shared_lock_num_++;
    auto lock_request=new LockRequest(txn->GetTransactionId(),LockMode::SHARED);
    lock_table_[*lock_data_id].request_queue_.push_back(*lock_request);
    lock_table_[*lock_data_id].cv_.notify_all();
    lock_set->insert(*lock_data_id);
    return true;
}

/**
 * 申请行级写锁
 * @param txn 要申请锁的事务对象指针
 * @param rid 加锁的目标记录ID
 * @param tab_fd 记录所在的表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockExclusiveOnRecord(Transaction *txn, const Rid &rid, int tab_fd) {
    if(txn->GetState()!=TransactionState::GROWING&&txn->GetState()!=TransactionState::DEFAULT){
        assert(0);
        return false;
    }
    txn->SetState(TransactionState::GROWING);
    auto lock_data_id=new LockDataId(tab_fd,rid,LockDataType::RECORD);
    std::unique_lock<std::mutex> lock(latch_);
    auto lock_set=txn->GetLockSet();
    int my_shared_lock_cnt=0;
    for(auto lock:*lock_set){
        if(lock.fd_==tab_fd&&lock.rid_==rid){
            for(auto i:lock_table_[*lock_data_id].request_queue_){
                //如果是写锁就return，如果是读锁则记录
                if(i.txn_id_==txn->GetTransactionId()){
                    if(i.lock_mode_==LockMode::EXLUCSIVE){
                        return true;
                    }
                    else{
                        my_shared_lock_cnt++;//最多只有一个
                    }
                    break;
                }
            }
        }
        break;
    }
    while(
        lock_table_[*lock_data_id].group_lock_mode_!=GroupLockMode::NON_LOCK&&
        my_shared_lock_cnt<lock_table_[*lock_data_id].request_queue_.size()
    ){
        lock_table_[*lock_data_id].cv_.wait(lock);
    }
    lock_table_[*lock_data_id].group_lock_mode_=GroupLockMode::X;
    if(!my_shared_lock_cnt){
        auto lock_request=new LockRequest(txn->GetTransactionId(),LockMode::EXLUCSIVE);
        lock_table_[*lock_data_id].request_queue_.push_back(*lock_request);
        lock_set->insert(*lock_data_id);
    }
    else{
        lock_table_[*lock_data_id].request_queue_.begin()->lock_mode_=LockMode::EXLUCSIVE;
    }
    lock_table_[*lock_data_id].cv_.notify_all();
    return true;
}

/**
 * 申请表级读锁
 * @param txn 要申请锁的事务对象指针
 * @param tab_fd 目标表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockSharedOnTable(Transaction *txn, int tab_fd) {
    auto lock_data_id=new LockDataId(tab_fd,LockDataType::TABLE);
    if(txn->GetState()!=TransactionState::GROWING&&txn->GetState()!=TransactionState::DEFAULT){
        assert(0);
        return false;
    }
    txn->SetState(TransactionState::GROWING);
    std::unique_lock<std::mutex> lock(latch_);
    auto lock_set=txn->GetLockSet();
    int status=0;
    for(auto lock:*lock_set){
        if(lock.fd_==tab_fd){
            //如果是X或者SIX或者S则return
            //如果是IS或者IX则分别标记
            for(auto i:lock_table_[*lock_data_id].request_queue_){
                if(i.txn_id_==txn->GetTransactionId()){
                    if(i.lock_mode_==LockMode::EXLUCSIVE||i.lock_mode_==LockMode::SHARED||
                    i.lock_mode_==LockMode::S_IX){
                        return true;
                    }
                    else{
                        if(i.lock_mode_==LockMode::INTENTION_SHARED){
                            status=1;
                        }
                        else if(i.lock_mode_==LockMode::INTENTION_EXCLUSIVE){
                            status=2;
                        }
                        break;
                    }
                }
            }
            break;
        }
    }

    while(lock_table_[*lock_data_id].group_lock_mode_!=GroupLockMode::S&&
        lock_table_[*lock_data_id].group_lock_mode_!=GroupLockMode::IS&&
        lock_table_[*lock_data_id].group_lock_mode_!=GroupLockMode::NON_LOCK&&
        !((status==2)&&lock_table_[*lock_data_id].IX_lock_num_==1)
    ){
        lock_table_[*lock_data_id].cv_.wait(lock);
    }
    
    if(status==0){
        auto lock_request=new LockRequest(txn->GetTransactionId(),LockMode::SHARED);
        lock_table_[*lock_data_id].request_queue_.push_back(*lock_request);
        lock_set->insert(*lock_data_id);
        lock_table_[*lock_data_id].group_lock_mode_=GroupLockMode::S;
        lock_table_[*lock_data_id].shared_lock_num_++;
    }
    else if(status==1){
        lock_table_[*lock_data_id].group_lock_mode_=GroupLockMode::S;
        lock_table_[*lock_data_id].request_queue_.begin()->lock_mode_=LockMode::SHARED;
        lock_table_[*lock_data_id].shared_lock_num_++;
    }
    else{
        lock_table_[*lock_data_id].group_lock_mode_=GroupLockMode::SIX;
        lock_table_[*lock_data_id].request_queue_.begin()->lock_mode_=LockMode::S_IX;
        lock_table_[*lock_data_id].IX_lock_num_--;
    }
    lock_table_[*lock_data_id].cv_.notify_all();
    return true;
}

/**
 * 申请表级写锁
 * @param txn 要申请锁的事务对象指针
 * @param tab_fd 目标表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockExclusiveOnTable(Transaction *txn, int tab_fd) {
    auto lock_data_id=new LockDataId(tab_fd,LockDataType::TABLE);
    if(txn->GetState()!=TransactionState::GROWING&&txn->GetState()!=TransactionState::DEFAULT){
        assert(0);
        return false;
    }
    txn->SetState(TransactionState::GROWING);
    std::unique_lock<std::mutex> lock(latch_);
    auto lock_set=txn->GetLockSet();
    int status=0;
    for(auto lock:*lock_set){
        if(lock.fd_==tab_fd){
            //如果是X则return
            //如果其他则同一标记（无区别）
            for(auto i:lock_table_[*lock_data_id].request_queue_){
                if(i.txn_id_==txn->GetTransactionId()){
                    if(i.lock_mode_==LockMode::EXLUCSIVE){
                        return true;
                    }
                    else{
                        status=1;
                        break;
                    }
                }
            }
            break;
        }
    }

    while(lock_table_[*lock_data_id].group_lock_mode_!=GroupLockMode::NON_LOCK&&
        status<lock_table_[*lock_data_id].request_queue_.size()
    ){
        lock_table_[*lock_data_id].cv_.wait(lock);
    }
    
    if(status==0){
        auto lock_request=new LockRequest(txn->GetTransactionId(),LockMode::EXLUCSIVE);
        lock_table_[*lock_data_id].request_queue_.push_back(*lock_request);
        lock_set->insert(*lock_data_id);
        lock_table_[*lock_data_id].group_lock_mode_=GroupLockMode::X;
    }
    else if(status==1){
        lock_table_[*lock_data_id].group_lock_mode_=GroupLockMode::X;
        lock_table_[*lock_data_id].request_queue_.begin()->lock_mode_=LockMode::EXLUCSIVE;
    }
    lock_table_[*lock_data_id].cv_.notify_all();
    return true;
}

/**
 * 申请表级意向读锁
 * @param txn 要申请锁的事务对象指针
 * @param tab_fd 目标表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockISOnTable(Transaction *txn, int tab_fd) {
    auto lock_data_id=new LockDataId(tab_fd,LockDataType::TABLE);
    if(txn->GetState()!=TransactionState::GROWING&&txn->GetState()!=TransactionState::DEFAULT){
        assert(0);
        return false;
    }
    txn->SetState(TransactionState::GROWING);
    std::unique_lock<std::mutex> lock(latch_);
    auto lock_set=txn->GetLockSet();
    for(auto lock:*lock_set){
        if(lock.fd_==tab_fd){
            //任意级别都>=IS,可以直接return
            return true;
        }
    }

    while(lock_table_[*lock_data_id].group_lock_mode_==GroupLockMode::X
    ){
        lock_table_[*lock_data_id].cv_.wait(lock);
    }
    auto lock_request=new LockRequest(txn->GetTransactionId(),LockMode::INTENTION_SHARED);
    lock_table_[*lock_data_id].request_queue_.push_back(*lock_request);
    lock_set->insert(*lock_data_id);
    if(lock_table_[*lock_data_id].group_lock_mode_==GroupLockMode::NON_LOCK)
        lock_table_[*lock_data_id].group_lock_mode_=GroupLockMode::IS;
    lock_table_[*lock_data_id].cv_.notify_all();
    return true;
}

/**
 * 申请表级意向写锁
 * @param txn 要申请锁的事务对象指针
 * @param tab_fd 目标表的fd
 * @return 返回加锁是否成功
 */
bool LockManager::LockIXOnTable(Transaction *txn, int tab_fd) {
    auto lock_data_id=new LockDataId(tab_fd,LockDataType::TABLE);
    if(txn->GetState()!=TransactionState::GROWING&&txn->GetState()!=TransactionState::DEFAULT){
        assert(0);
        return false;
    }
    txn->SetState(TransactionState::GROWING);
    std::unique_lock<std::mutex> lock(latch_);
    auto lock_set=txn->GetLockSet();
    int status=0;
    for(auto lock:*lock_set){
        if(lock.fd_==tab_fd){
            //如果是IX或者SIX或者X则return
            //如果是IS或者S则分别标记
            for(auto i:lock_table_[*lock_data_id].request_queue_){
                if(i.txn_id_==txn->GetTransactionId()){
                    if(i.lock_mode_==LockMode::EXLUCSIVE||i.lock_mode_==LockMode::INTENTION_EXCLUSIVE||
                    i.lock_mode_==LockMode::S_IX){
                        return true;
                    }
                    else{
                        if(i.lock_mode_==LockMode::INTENTION_SHARED){
                            status=1;
                        }
                        else if(i.lock_mode_==LockMode::SHARED){
                            status=2;
                        }
                        break;
                    }
                }
            }
            break;
        }
    }

    while(lock_table_[*lock_data_id].group_lock_mode_!=GroupLockMode::IX&&
        lock_table_[*lock_data_id].group_lock_mode_!=GroupLockMode::IS&&
        lock_table_[*lock_data_id].group_lock_mode_!=GroupLockMode::NON_LOCK&&
        !(status==2&&lock_table_[*lock_data_id].shared_lock_num_==1)
    ){
        lock_table_[*lock_data_id].cv_.wait(lock);
    }
    
    if(status==0){
        auto lock_request=new LockRequest(txn->GetTransactionId(),LockMode::INTENTION_EXCLUSIVE);
        lock_table_[*lock_data_id].request_queue_.push_back(*lock_request);
        lock_set->insert(*lock_data_id);
        lock_table_[*lock_data_id].group_lock_mode_=GroupLockMode::IX;
        lock_table_[*lock_data_id].IX_lock_num_++;
    }
    else if(status==1){
        lock_table_[*lock_data_id].group_lock_mode_=GroupLockMode::IX;
        lock_table_[*lock_data_id].request_queue_.begin()->lock_mode_=LockMode::INTENTION_EXCLUSIVE;
        lock_table_[*lock_data_id].IX_lock_num_++;
    }
    else{
        lock_table_[*lock_data_id].group_lock_mode_=GroupLockMode::SIX;
        lock_table_[*lock_data_id].request_queue_.begin()->lock_mode_=LockMode::S_IX;
        lock_table_[*lock_data_id].shared_lock_num_--;
    }
    
    lock_table_[*lock_data_id].cv_.notify_all();
    
    return true;
}

/**
 * 释放锁
 * @param txn 要释放锁的事务对象指针
 * @param lock_data_id 要释放的锁ID
 * @return 返回解锁是否成功
 */
bool LockManager::Unlock(Transaction *txn, LockDataId lock_data_id) {

    if(txn->GetState()!=TransactionState::SHRINKING&&txn->GetState()!=TransactionState::GROWING){
        assert(0);
        return false;
    }
    txn->SetState(TransactionState::SHRINKING);
    std::unique_lock<std::mutex> lock(latch_);
    auto lock_set=txn->GetLockSet();
    bool judge=false;
    for(auto i:*lock_set){
        if(i==lock_data_id){
            judge=true;
            break;
        }
    }
    if(!judge)assert(0);
    std::vector<LockManager::LockRequest> v;
    for(auto i:lock_table_[lock_data_id].request_queue_){
        if(i.txn_id_==txn->GetTransactionId()){
            if(i.lock_mode_==LockMode::EXLUCSIVE){
                lock_table_[lock_data_id].group_lock_mode_=GroupLockMode::NON_LOCK;
            }
            else if(i.lock_mode_==LockMode::INTENTION_EXCLUSIVE){
                lock_table_[lock_data_id].IX_lock_num_--;
                if(!lock_table_[lock_data_id].IX_lock_num_){
                    if(lock_table_[lock_data_id].request_queue_.size()>1){
                        lock_table_[lock_data_id].group_lock_mode_=GroupLockMode::IS;
                    }
                    else lock_table_[lock_data_id].group_lock_mode_=GroupLockMode::NON_LOCK;
                }
            }
            else if(i.lock_mode_==LockMode::SHARED){
                lock_table_[lock_data_id].shared_lock_num_--;
                if(!lock_table_[lock_data_id].shared_lock_num_){
                    if(lock_table_[lock_data_id].request_queue_.size()>1){
                        lock_table_[lock_data_id].group_lock_mode_=GroupLockMode::IS;
                    }
                    else lock_table_[lock_data_id].group_lock_mode_=GroupLockMode::NON_LOCK;
                }
            }
            else if(i.lock_mode_==LockMode::INTENTION_SHARED){
                if(!lock_table_[lock_data_id].shared_lock_num_){
                    if(lock_table_[lock_data_id].request_queue_.size()==1){
                        lock_table_[lock_data_id].group_lock_mode_=GroupLockMode::NON_LOCK;
                    }
                }
            }
            else if(i.lock_mode_==LockMode::S_IX){
                if(lock_table_[lock_data_id].request_queue_.size()>1){
                    lock_table_[lock_data_id].group_lock_mode_=GroupLockMode::IS;
                }
                else lock_table_[lock_data_id].group_lock_mode_=GroupLockMode::NON_LOCK;
            }
        }
        else{
            v.push_back(i);
        }
    }
    lock_table_[lock_data_id].request_queue_.clear();
    for(auto i:v){
        lock_table_[lock_data_id].request_queue_.push_back(i);
    }
    lock_table_[lock_data_id].cv_.notify_all();
    return true;
}