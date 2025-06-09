#pragma once

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstddef>
#include <stdexcept>
#include <string>

namespace kvstore
{
class FileReader
{
public:
    FileReader(const std::string &fname) : file_name_(fname)
    {
        fd_ = open(fname.c_str(), O_RDONLY | O_DIRECT);
        if (fd_ < 0)
        {
            throw std::runtime_error("Fail to open file");
        }

        file_size_ = GetFileSize(fd_);
        if (file_size_ < 0)
        {
            throw std::runtime_error("Fail to get size of file");
        }
    }

    FileReader(const FileReader &) = delete;

    FileReader(FileReader &&rhs)
        : file_name_(rhs.file_name_), fd_(rhs.fd_), file_size_(rhs.file_size_)
    {
    }

    ~FileReader()
    {
        if (fd_)
        {
            close(fd_);
        }
    }

    int Fd() const
    {
        return fd_;
    }

    size_t Size() const
    {
        return file_size_;
    }

private:
    size_t GetFileSize(int fd)
    {
        struct stat st;

        if (fstat(fd, &st) < 0)
        {
            perror("fstat");
            return -1;
        }

        if (S_ISBLK(st.st_mode))
        {
            unsigned long long bytes;
            if (ioctl(fd, BLKGETSIZE64, &bytes) != 0)
            {
                perror("ioctl");
                return -1;
            }
            return bytes;
        }
        else if (S_ISREG(st.st_mode))
            return st.st_size;

        return -1;
    }

    std::string file_name_;
    int fd_;
    size_t file_size_;
};
}  // namespace kvstore