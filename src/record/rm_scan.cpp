#include "rm_scan.h"

#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 *
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    file_handle_=file_handle;
    rid_.page_no=1;
    rid_.slot_no=-1;
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

    if(is_end())return;
    while(1){
        int pos=Bitmap::next_bit(1,file_handle_->fetch_page_handle(rid_.page_no).bitmap,file_handle_->file_hdr_.num_records_per_page,rid_.slot_no);
        if(file_handle_->file_hdr_.num_records_per_page!=pos){
            rid_.slot_no=pos;
            return;
        }
        rid_.slot_no=-1;
        if(++rid_.page_no>=file_handle_->file_hdr_.num_pages)return;
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    return(rid_.page_no==file_handle_->file_hdr_.num_pages);
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    // Todo: 修改返回值
    return rid_;
}