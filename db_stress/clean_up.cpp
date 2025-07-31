#include <gflags/gflags.h>

#include <filesystem>
#include <iostream>

// due to the history ,this file need not exit
DEFINE_string(store_path, "data/", "path to the test folder ");

int main()
{
    std::string db_stress_helper = "db_stress_helper";
    std::string stress_test = "stress_test";

    std::string db_stress_helper_path = FLAGS_store_path + db_stress_helper;
    std::string stress_test_path = FLAGS_store_path + stress_test;

    try
    {
        // Check if db_stress_helper folder exists
        if (std::filesystem::exists(db_stress_helper_path))
        {
            std::cout << "Found folder: " << db_stress_helper
                      << ", attempting to delete..." << std::endl;
            std::filesystem::remove_all(db_stress_helper_path);

            // Verify deletion was successful
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

        // Check if stress_test folder exists
        if (std::filesystem::exists(stress_test_path))
        {
            std::cout << "Found folder: " << stress_test
                      << ", attempting to delete..." << std::endl;
            std::filesystem::remove_all(stress_test_path);

            // Verify deletion was successful
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