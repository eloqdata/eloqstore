#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <iostream>

int main()
{
    const char *filename = "test_delete.txt";

    // 1. 写入一个文件
    int fd_create = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    write(fd_create, "HELLO_FROM_FILE\n", 16);
    close(fd_create);

    // 2. 再次打开它用于读
    int fd = open(filename, O_RDONLY);
    if (fd < 0)
    {
        perror("open");
        return 1;
    }

    std::cout << "文件已通过 fd 打开，现在 rm 它。\n";

    // 3. 删除文件（unlink）
    unlink(filename);

    std::cout << "文件已删除，但 fd 仍然持有。\n\n";

    // 4. 尝试从 fd 读取内容
    char buf[256] = {0};
    ssize_t n = read(fd, buf, sizeof(buf));

    std::cout << "通过 fd 读取到内容（文件已经 rm）:\n";
    std::cout << buf << "\n";

    // 5. 暂停一下，让你用 lsof 观察
    std::cout << "程序暂停 20 秒，运行: lsof | grep deleted\n";
    sleep(20);

    close(fd);
    std::cout << "fd 已关闭，文件真正从磁盘释放。\n";

    return 0;
}
