#include "obj_gen.h"

#include <cassert>
#include <cmath>
// #include <unistd.h>
// #include <string>
#include <fcntl.h>

#include <cstring>
// #include <filesystem>
// #include <fstream>
#include <glog/logging.h>

#include <memory>

namespace EloqStoreBM
{
random_generator::random_generator()
{
    set_seed(0);
}

void random_generator::set_seed(int seed)
{
    seed++;  // http://stackoverflow.com/questions/27386470/srand0-and-srand1-give-the-same-results
#ifdef HAVE_RANDOM_R
    memset(&m_data_blob, 0, sizeof(m_data_blob));
    memset(m_state_array, 0, sizeof(m_state_array));

    int ret =
        initstate_r(seed, m_state_array, sizeof(m_state_array), &m_data_blob);
    assert(ret == 0);
#elif (defined HAVE_DRAND48)
    memset(&m_data_blob, 0, sizeof(m_data_blob));
    size_t seed_size =
        sizeof(seed);  // get MIN size between seed and m_data_blob
    if (seed_size > sizeof(m_data_blob))
        seed_size = sizeof(m_data_blob);
    memcpy(&m_data_blob, &seed, seed_size);
#endif
}

unsigned long long random_generator::get_random()
{
    unsigned long long llrn;
#ifdef HAVE_RANDOM_R
    int32_t rn;
    // max is RAND_MAX, which is usually 2^31-1 (although can be as low as
    // 2^16-1, which we ignore now) this is fine, especially considering that
    // random_r is a nonstandard glibc extension it returns a positive int32_t,
    // so either way the MSB is off
    int ret = random_r(&m_data_blob, &rn);
    assert(ret == 0);
    llrn = rn;
    llrn = llrn << 31;

    ret = random_r(&m_data_blob, &rn);
    assert(ret == 0);
    llrn |= rn;
#elif (defined HAVE_DRAND48)
    long rn;
    // jrand48's range is -2^31..+2^31 (i.e. all 32 bits)
    rn = jrand48(m_data_blob);
    llrn = rn;
    llrn = llrn << 32;

    rn = jrand48(m_data_blob);
    llrn |=
        rn & 0xffffffff;  // reset the sign extension bits of negative numbers
    llrn &= 0x7FFFFFFFFFFFFFFF;  // avoid any trouble from sign mismatch and
                                 // negative numbers
#else
    assert(false && "no random function");
#endif
    return llrn;
}

unsigned long long random_generator::get_random_max() const
{
#ifdef HAVE_RANDOM_R
    return 0x3fffffffffffffff;  // 62 bits
#elif (defined HAVE_DRAND48)
    return 0x7fffffffffffffff;  // 63 bits
#endif
}

// returns a value surrounding 0
double gaussian_noise::gaussian_distribution(const double &stddev)
{
    // Box–Muller transform (Marsaglia polar method)
    if (m_hasSpare)
    {
        m_hasSpare = false;
        return stddev * m_spare;
    }

    m_hasSpare = true;
    double u, v, s;
    do
    {
        u = (get_random() / ((double) get_random_max())) * 2 - 1;
        v = (get_random() / ((double) get_random_max())) * 2 - 1;
        s = u * u + v * v;
    } while (s >= 1 || s == 0);

    s = sqrt(-2.0 * log(s) / s);
    m_spare = v * s;
    return stddev * u * s;
}

unsigned long long gaussian_noise::gaussian_distribution_range(
    double stddev,
    double median,
    unsigned long long min,
    unsigned long long max)
{
    if (min == max)
        return min;

    unsigned long long len = max - min;

    double val;
    if (median == 0)
        median = len / 2.0 + min + 0.5;
    if (stddev == 0)
        stddev = len / 6.0;
    assert(median > min && median < max);
    do
    {
        val = gaussian_distribution(stddev) + median;
    } while (val < min || val > max + 1);
    return val;
}

object_generator::object_generator(
    size_t n_key_iterators /*= OBJECT_GENERATOR_KEY_ITERATORS*/)
    : m_key_size(0),
      m_magic_key_size(0),
      m_data_size_type(data_size_unknown),
      m_data_size_pattern(NULL),
      m_random_data(false),
      m_expiry_min(0),
      m_expiry_max(0),
      m_key_prefix(NULL),
      m_key_prefix_len(0),
      m_key_min(0),
      m_key_max(0),
      m_key_stddev(0),
      m_key_median(0),
      m_value_buffer(NULL),
      m_random_fd(-1),
      m_value_buffer_size(0),
      m_value_buffer_mutation_pos(0)
{
    m_next_key.resize(n_key_iterators, 0);

    // m_data_size.size_list = NULL;

    memset(m_key_buffer, 0, 250);

    // Check the endian
    unsigned short endian = 0x0A0B;
    const char *first = reinterpret_cast<const char *>(&endian);
    if (*first == 0x0A)
    {
        assert(std::endian::native == std::endian::big);
        DLOG(INFO) << "big-endian";
    }
    else
    {
        assert(std::endian::native == std::endian::little);
        DLOG(INFO) << "little-endian";
    }
}

object_generator::object_generator(bool random_data,
                                   unsigned int key_size,
                                   unsigned int fixed_size,
                                   const char *key_prefix,
                                   unsigned long long key_min,
                                   unsigned long long key_max,
                                   size_t n_key_iterators)
    : m_key_size(key_size > 250 ? 250 : key_size),
      m_magic_key_size(m_key_size > 8 ? (m_key_size - 8) : 0),
      m_data_size_type(data_size_fixed),
      m_data_size_pattern(NULL),
      m_random_data(random_data),
      m_expiry_min(0),
      m_expiry_max(0),
      m_key_prefix(key_prefix),
      m_key_min(key_min),
      m_key_max(key_max),
      m_key_stddev(0),
      m_key_median(0),
      m_value_buffer(NULL),
      m_random_fd(-1),
      m_value_buffer_size(0),
      m_value_buffer_mutation_pos(0)
{
    m_next_key.resize(n_key_iterators, 0);

    m_data_size.size_fixed = fixed_size;
    if (!alloc_value_buffer())
    {
        exit(-1);
    }

    int width = 1;
    uint64_t temp = m_key_max;
    while (temp >= 10)
    {
        ++width;
        temp /= 10;
    }
    m_key_format.append(std::to_string(width)).append("llu");

    memset(m_key_buffer, 0, 250);
    m_key_prefix_len = strlen(m_key_prefix);
    memcpy(m_key_buffer, m_key_prefix, m_key_prefix_len);

    // set magic data
    char *dest = m_key_buffer + m_key_prefix_len;
    memset(dest, 0xFF, m_magic_key_size);

    // Check the endian
    unsigned short endian = 0x0A0B;
    const char *first = reinterpret_cast<const char *>(&endian);
    if (*first == 0x0A)
    {
        assert(std::endian::native == std::endian::big);
        DLOG(INFO) << "big-endian";
    }
    else
    {
        assert(std::endian::native == std::endian::little);
        DLOG(INFO) << "little-endian";
    }
}

/*
object_generator::object_generator(const object_generator &copy)
    : m_key_size(copy.m_key_size),
      m_data_size_type(copy.m_data_size_type),
      m_data_size(copy.m_data_size),
      m_data_size_pattern(copy.m_data_size_pattern),
      m_random_data(copy.m_random_data),
      m_expiry_min(copy.m_expiry_min),
      m_expiry_max(copy.m_expiry_max),
      m_key_prefix(copy.m_key_prefix),
      m_key_prefix_len(copy.m_key_prefix_len),
      m_key_min(copy.m_key_min),
      m_key_max(copy.m_key_max),
      m_key_stddev(copy.m_key_stddev),
      m_key_median(copy.m_key_median),
      m_value_buffer(NULL),
      m_random_fd(-1),
      m_value_buffer_size(0),
      m_value_buffer_mutation_pos(0)
{
    // if (m_data_size_type == data_size_weighted &&
    //     m_data_size.size_list != NULL)
    // {
    //     m_data_size.size_list = new
    //     config_weight_list(*m_data_size.size_list);
    // }
    alloc_value_buffer(copy.m_value_buffer);

    m_next_key.resize(copy.m_next_key.size(), 0);

    memset(m_key_buffer, 0, 250);
    memcpy(m_key_buffer,
           copy.m_key_buffer,
           copy.m_key_prefix_len + copy.m_key_size);
}
*/

object_generator::object_generator(object_generator &&rhs)
    : m_key_size(rhs.m_key_size),
      m_magic_key_size(rhs.m_magic_key_size),
      m_data_size_type(rhs.m_data_size_type),
      m_data_size(rhs.m_data_size),
      m_data_size_pattern(rhs.m_data_size_pattern),
      m_random_data(rhs.m_random_data),
      m_expiry_min(rhs.m_expiry_min),
      m_expiry_max(rhs.m_expiry_max),
      m_key_prefix(rhs.m_key_prefix),
      m_key_prefix_len(rhs.m_key_prefix_len),
      m_key_min(rhs.m_key_min),
      m_key_max(rhs.m_key_max),
      m_key_stddev(rhs.m_key_stddev),
      m_key_median(rhs.m_key_median),
      m_next_key(std::move(rhs.m_next_key)),
      m_value_buffer(rhs.m_value_buffer),
      m_random_fd(rhs.m_random_fd),
      m_value_buffer_size(rhs.m_value_buffer_size),
      m_value_buffer_mutation_pos(rhs.m_value_buffer_mutation_pos)
{
    // if (m_data_size_type == data_size_weighted &&
    //     m_data_size.size_list != NULL)
    // {
    //     m_data_size.size_list = new
    //     config_weight_list(*m_data_size.size_list);
    // }
    memset(m_key_buffer, 0, 250);
    memcpy(m_key_buffer, rhs.m_key_buffer, 250);

    rhs.m_value_buffer = nullptr;
    rhs.m_random_fd = -1;
}

object_generator::~object_generator()
{
    if (m_value_buffer != NULL)
        free(m_value_buffer);
    // if (m_data_size_type == data_size_weighted &&
    //     m_data_size.size_list != NULL)
    // {
    //     delete m_data_size.size_list;
    // }
    if (m_random_fd != -1)
    {
        close(m_random_fd);
        m_random_fd = -1;
    }
}

object_generator *object_generator::clone(void)
{
    assert(false);
    return nullptr;
    // return new object_generator(*this);
}

void object_generator::set_random_seed(int seed)
{
    m_random.set_seed(seed);
}

bool object_generator::alloc_value_buffer(void)
{
    unsigned int size = 0;

    if (m_value_buffer != NULL)
        free(m_value_buffer), m_value_buffer = NULL;

    if (m_data_size_type == data_size_fixed)
        size = m_data_size.size_fixed;
    else if (m_data_size_type == data_size_range)
        size = m_data_size.size_range.size_max;
    // else if (m_data_size_type == data_size_weighted)
    //     size = m_data_size.size_list->largest();

    m_value_buffer_size = size;
    if (size > 0)
    {
        m_value_buffer = (char *) malloc(size);
        assert(m_value_buffer != NULL);
        if (!m_random_data)
        {
            memset(m_value_buffer, 'x', size);
        }
        else
        {
            if (m_random_fd == -1)
            {
                m_random_fd = open("/dev/urandom", O_RDONLY);
                if (m_random_fd == -1)
                {
                    LOG(ERROR) << "open urandom errno: " << errno;
                    return false;
                }
                assert(m_random_fd != -1);
            }

            char buf1[64] = {0};
            char buf2[64] = {0};
            unsigned int buf1_idx = sizeof(buf1);
            unsigned int buf2_idx = sizeof(buf2);
            char *d = m_value_buffer;
            int ret;
            int iter = 0;
            while (d - m_value_buffer < size)
            {
                if (buf1_idx == sizeof(buf1))
                {
                    buf1_idx = 0;
                    buf2_idx++;

                    if (buf2_idx >= sizeof(buf2))
                    {
                        if (iter % 20 == 0)
                        {
                            ret = read(m_random_fd, buf1, sizeof(buf1));
                            assert(ret > -1);
                            ret = read(m_random_fd, buf2, sizeof(buf2));
                            assert(ret > -1);
                        }
                        buf2_idx = 0;
                        iter++;
                    }
                }
                *d = buf1[buf1_idx] ^ buf2[buf2_idx] ^ iter;
                d++;
                buf1_idx++;
            }
        }
    }
    return true;
}

void object_generator::alloc_value_buffer(const char *copy_from)
{
    unsigned int size = 0;

    if (m_value_buffer != NULL)
        free(m_value_buffer), m_value_buffer = NULL;

    if (m_data_size_type == data_size_fixed)
        size = m_data_size.size_fixed;
    else if (m_data_size_type == data_size_range)
        size = m_data_size.size_range.size_max;
    else if (m_data_size_type == data_size_weighted)
    {
        assert(false);
        // size = m_data_size.size_list->largest();
    }

    m_value_buffer_size = size;
    if (size > 0)
    {
        m_value_buffer = (char *) malloc(size);
        assert(m_value_buffer != NULL);
        memcpy(m_value_buffer, copy_from, size);
    }
}

void object_generator::set_random_data(bool random_data)
{
    m_random_data = random_data;
}

void object_generator::set_key_size(unsigned int size)
{
    m_key_size = size > 250 ? 250 : size;
    m_magic_key_size = m_key_size > 8 ? (m_key_size - 8) : 0;
}

void object_generator::set_data_size_fixed(unsigned int size)
{
    m_data_size_type = data_size_fixed;
    m_data_size.size_fixed = size;
    if (!alloc_value_buffer())
    {
        exit(-1);
    }
}

void object_generator::set_data_size_range(unsigned int size_min,
                                           unsigned int size_max)
{
    m_data_size_type = data_size_range;
    m_data_size.size_range.size_min = size_min;
    m_data_size.size_range.size_max = size_max;
    if (!alloc_value_buffer())
    {
        exit(-1);
    }
}

// void object_generator::set_data_size_list(config_weight_list *size_list)
// {
//     if (m_data_size_type == data_size_weighted && m_data_size.size_list !=
//     NULL)
//     {
//         delete m_data_size.size_list;
//     }
//     m_data_size_type = data_size_weighted;
//     m_data_size.size_list = new config_weight_list(*size_list);
//     alloc_value_buffer();
// }

void object_generator::set_data_size_pattern(const char *pattern)
{
    m_data_size_pattern = pattern;
}

void object_generator::set_expiry_range(unsigned int expiry_min,
                                        unsigned int expiry_max)
{
    m_expiry_min = expiry_min;
    m_expiry_max = expiry_max;
}

void object_generator::set_key_prefix(const char *key_prefix)
{
    m_key_prefix = key_prefix;
    m_key_prefix_len = strlen(key_prefix);

    memcpy(m_key_buffer, m_key_prefix, m_key_prefix_len);

    // set magic data
    char *dest = m_key_buffer + m_key_prefix_len;
    memset(dest, 0xFF, m_magic_key_size);
}

void object_generator::set_key_range(unsigned long long key_min,
                                     unsigned long long key_max)
{
    m_key_min = key_min;
    m_key_max = key_max;

    int width = 1;
    uint64_t temp = m_key_max;
    while (temp >= 10)
    {
        ++width;
        temp /= 10;
    }
    m_key_format.append(std::to_string(width)).append("llu");
}

void object_generator::set_key_distribution(double key_stddev,
                                            double key_median)
{
    m_key_stddev = key_stddev;
    m_key_median = key_median;
}

// return a random number between r_min and r_max
unsigned long long object_generator::random_range(unsigned long long r_min,
                                                  unsigned long long r_max)
{
    unsigned long long rn = m_random.get_random();
    return (rn % (r_max - r_min + 1)) + r_min;
}

// return a random number between r_min and r_max using normal distribution
// according to r_stddev
unsigned long long object_generator::normal_distribution(
    unsigned long long r_min,
    unsigned long long r_max,
    double r_stddev,
    double r_median)
{
    return m_random.gaussian_distribution_range(
        r_stddev, r_median, r_min, r_max);
}

unsigned long long object_generator::get_key_index(int iter)
{
    assert(iter < static_cast<int>(m_next_key.size()) &&
           iter >= OBJECT_GENERATOR_KEY_GAUSSIAN);

    unsigned long long k;
    if (iter == OBJECT_GENERATOR_KEY_RANDOM)
    {
        k = random_range(m_key_min, m_key_max);
    }
    else if (iter == OBJECT_GENERATOR_KEY_GAUSSIAN)
    {
        k = normal_distribution(
            m_key_min, m_key_max, m_key_stddev, m_key_median);
    }
    else
    {
        if (m_next_key[iter] < m_key_min)
            m_next_key[iter] = m_key_min;
        k = m_next_key[iter];

        m_next_key[iter]++;
        if (m_next_key[iter] > m_key_max)
            m_next_key[iter] = m_key_min;
    }
    return k;
}

void object_generator::generate_key(unsigned long long key_index)
{
    // m_key_len = snprintf(m_key_buffer,
    //                      sizeof(m_key_buffer) - 1,
    //                      m_key_format.c_str(),
    //                      m_key_prefix,
    //                      key_index);

    assert(std::endian::native == std::endian::little);
    const char *index_ptr =
        static_cast<const char *>(static_cast<void *>(&key_index));
    char *dest = m_key_buffer + m_key_prefix_len + m_magic_key_size;
    unsigned int actual_cnt = m_key_size - m_magic_key_size;
    for (unsigned int i = actual_cnt; i > 0; --i)
    {
        dest[actual_cnt - i] = index_ptr[i - 1];
    }
    m_key_buffer[m_key_prefix_len + m_key_size] = '\0';

    m_key_len = m_key_prefix_len + m_key_size;
    m_key = m_key_buffer;
}

void object_generator::generate_key(uint64_t key_index, std::string &dest)
{
    assert(dest.size() == 0);
    // The prefix
    dest.append(m_key_buffer, (m_key_prefix_len + m_magic_key_size));
    // The actual value
    const char *index_ptr =
        static_cast<const char *>(static_cast<void *>(&key_index));
    unsigned int actual_cnt = m_key_size - m_magic_key_size;
    for (unsigned int i = actual_cnt; i > 0; --i)
    {
        dest.append((index_ptr + (i - 1)), 1);
    }

    m_key_len = m_key_prefix_len + m_key_size;
    assert(dest.size() == m_key_len);
}

const char *object_generator::get_key_prefix()
{
    return m_key_prefix;
}

const char *object_generator::get_value(unsigned long long key_index,
                                        unsigned int *len)
{
    // compute size
    unsigned int new_size = 0;
    if (m_data_size_type == data_size_fixed)
    {
        new_size = m_data_size.size_fixed;
    }
    else if (m_data_size_type == data_size_range)
    {
        if (m_data_size_pattern && *m_data_size_pattern == 'S')
        {
            double a = (key_index - m_key_min) /
                       static_cast<double>(m_key_max - m_key_min);
            new_size = (m_data_size.size_range.size_max -
                        m_data_size.size_range.size_min) *
                           a +
                       m_data_size.size_range.size_min;
        }
        else
        {
            new_size = random_range(m_data_size.size_range.size_min > 0
                                        ? m_data_size.size_range.size_min
                                        : 1,
                                    m_data_size.size_range.size_max);
        }
    }
    else if (m_data_size_type == data_size_weighted)
    {
        assert(false);
        // new_size = m_data_size.size_list->get_next_size();
    }
    else
    {
        assert(0);
    }

    // modify object content in case of random data
    if (m_random_data)
    {
        m_value_buffer[m_value_buffer_mutation_pos++]++;
        if (m_value_buffer_mutation_pos >= m_value_buffer_size)
            m_value_buffer_mutation_pos = 0;
    }

    *len = new_size;
    return m_value_buffer;
}

unsigned int object_generator::get_expiry()
{
    // compute expiry
    unsigned int expiry = 0;
    if (m_expiry_max > 0)
    {
        expiry = random_range(m_expiry_min, m_expiry_max);
    }

    return expiry;
}

}  // namespace EloqStoreBM