#include <mdbx.h++>
#include <iostream>
#include <string>
#include <vector>

int main() {
    try {
        // 创建环境参数
        mdbx::env_managed::create_parameters create_params;
        create_params.geometry.make_dynamic(1 * mdbx::env::geometry::GiB);
        
        // 操作参数
        mdbx::env_managed::operate_parameters operate_params;
        operate_params.max_maps = 16;
        
        // 创建环境
        mdbx::env_managed env("/tmp/test_mdbx", create_params, operate_params);
        
        // 创建数据库
        auto txn = env.start_write();
        // mdbx::value_mode::multi would enable MDBX_DUPSORT
        auto dbi = txn.create_map("test", mdbx::key_mode::usual, mdbx::value_mode::multi);
        
        // 插入数据
        std::string key1 = "Vitalik";
        std::string value1 = "Buterin";
        std::cout << "Inserting key1: " << key1 << " with value: " << value1 << std::endl;
        txn.upsert(dbi, {key1.data(), key1.size()}, {value1.data(), value1.size()});
        
        std::string key2 = "Satoshi";
        std::string value2 = "Nakamoto";
        std::cout << "Inserting key2: " << key2 << " with value: " << value2 << std::endl;
        txn.upsert(dbi, {key2.data(), key2.size()}, {value2.data(), value2.size()});
        
        std::cout << "Committing transaction" << std::endl;
        txn.commit();
        
        // 读取数据
        auto read_txn = env.start_read();
        auto cursor = read_txn.open_cursor(dbi);
        
        // 查找第一个键
        auto result = cursor.to_first();
        if (result.done) {
            std::string found_key(reinterpret_cast<const char*>(result.key.data()), result.key.size());
            std::string found_value(reinterpret_cast<const char*>(result.value.data()), result.value.size());
            std::cout << "Found: " << found_key << " = " << found_value << std::endl;
        } else {
            std::cout << "No data found for first key " << key1 << std::endl;
        }
        
        // 查找特定键. 使用MDBX_PREV_DUP
        result = cursor.move(mdbx::cursor::multi_currentkey_prevvalue, true);
        if (result.done) {
            std::string found_value(reinterpret_cast<const char*>(result.value.data()), result.value.size());
            std::cout << "Found key1: " << found_value << std::endl;
        } else {
            std::cout << "No data found for key1 " << key1 << std::endl;
        }
        
        std::cout << "MDBX test completed successfully!" << std::endl;
        
    } catch (const mdbx::exception& e) {
        std::cerr << "MDBX error: " << e.what() << std::endl;
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
