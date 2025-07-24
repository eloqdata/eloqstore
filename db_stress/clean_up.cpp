#include <gflags/gflags.h>

#include <filesystem>
#include <iostream>

DEFINE_string(store_path, "data/", "path to the test folder ");

int main()
{
    std::string db_stress_helper = "db_stress_helper";
    std::string stress_test = "stress_test";

    std::string db_stress_helper_path = FLAGS_store_path + db_stress_helper;
    std::string stress_test_path = FLAGS_store_path + stress_test;

    try
    {
        // 验证db_stress_helper1文件夹是否存在
        if (std::filesystem::exists(db_stress_helper_path))
        {
            std::cout << "找到文件了: " << db_stress_helper << ", 尝试删除..."
                      << std::endl;
            std::filesystem::remove_all(db_stress_helper_path);

            // 验证删除是否成功
            if (!std::filesystem::exists(db_stress_helper_path))
            {
                std::cout << "Folder: " << db_stress_helper
                          << " has been successfully deleted." << std::endl;
            }
            else
            {
                std::cerr << "Error: Failed to delete folder "
                          << db_stress_helper << std::endl;
                return 1;
            }
        }
        else
        {
            std::cout << "Folder: " << db_stress_helper
                      << " does not exist, skipping deletion." << std::endl;
        }

        // 验证stress_test1文件夹是否存在
        if (std::filesystem::exists(stress_test_path))
        {
            std::cout << "找到文件了: " << stress_test << ", 尝试删除..."
                      << std::endl;
            std::filesystem::remove_all(stress_test_path);

            // 验证删除是否成功
            if (!std::filesystem::exists(stress_test_path))
            {
                std::cout << "Folder: " << stress_test
                          << " has been successfully deleted." << std::endl;
            }
            else
            {
                std::cerr << "Error: Failed to delete folder " << stress_test
                          << std::endl;
                return 1;
            }
        }
        else
        {
            std::cout << "Folder: " << stress_test
                      << " does not exist, skipping deletion." << std::endl;
        }
    }
    catch (const std::filesystem::filesystem_error &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}