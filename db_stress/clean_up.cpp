#include <gflags/gflags.h>

#include <filesystem>
#include <iostream>

DEFINE_string(store_path, "/tmp/", "path to the test folder ");

int main()
{
    std::string db_stress_helper = "db_stress_helper";
    std::string stress_test = "stress_test";
    try
    {
        std::filesystem::remove_all(FLAGS_store_path + db_stress_helper);
        std::cout << "Folder:db_stress_helper has been deleted." << std::endl;
        std::filesystem::remove_all(FLAGS_store_path + stress_test);
        std::cout << "Folder:stress_test has been deleted." << std::endl;
    }
    catch (const std::filesystem::filesystem_error &e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    return 0;
}