#ifndef PARALLAXSERDES_H
#define PARALLAXSERDES_H
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <string>
#include "eckit/persist/DumpLoad.h"

#define SERDES_FATAL(...)                                                    \
    do {                                                                     \
        char buffer[1024];                                                   \
        snprintf(buffer, sizeof(buffer), __VA_ARGS__);                       \
        std::cerr << __FILE__ << ":" << __func__ << ":" << __LINE__ << " " \
                    << " FATAL(unimplemented): " << buffer << "\n";          \
        _exit(EXIT_FAILURE);                                                 \
    } while (0);

namespace fdb5 {


template <std::size_t T>
class ParallaxSerDes : public eckit::DumpLoad {
public:
    ParallaxSerDes() { buffer_.fill(0); }
    ~ParallaxSerDes() = default;
    size_t getSize() const { return this->buffer_size_; }
    const char* getBuffer() const { return this->buffer_.data(); }

private:
    virtual void
    beginObject(const std::string& str);
    virtual void endObject();
    virtual void nullObject();
    virtual std::string nextObject();
    virtual void doneObject();
    virtual void reset();
    virtual void load(std::string& str);
    virtual void load(float& a);
    virtual void load(double& a);
    virtual void load(int& a);
    virtual void load(unsigned int& a);
    virtual void load(long& a);
    virtual void load(unsigned long& a);
    virtual void load(long long& a);
    virtual void load(unsigned long long& a);
    virtual void load(char& str);
    virtual void load(unsigned char& str);


    virtual void dump(const std::string& str);
    virtual void dump(float a);
    virtual void dump(double a);
    virtual void dump(int a);
    virtual void dump(unsigned int a);
    virtual void dump(long a);
    virtual void dump(unsigned long a);
    virtual void dump(long long a);
    virtual void dump(unsigned long long a);
    virtual void dump(char str);
    virtual void dump(unsigned char str);
    virtual void push(const std::string& str1, const std::string& str2);
    virtual std::string get(const std::string& str1);
    virtual void pop(const std::string& str);
    void inner_dump(void* ptr, size_t size);
    // members
    std::array<char, T> buffer_;
    size_t buffer_size_;
};

template <std::size_t T>
void ParallaxSerDes<T>::beginObject(const std::string& str) {
    this->buffer_size_ = 0;
}

template <std::size_t T>
void ParallaxSerDes<T>::endObject() {
    ;
}

template <std::size_t T>
void ParallaxSerDes<T>::nullObject() {
    SERDES_FATAL("lala")
}

template <std::size_t T>
std::string ParallaxSerDes<T>::nextObject() {
    SERDES_FATAL("lala")
    return "";
}

template <std::size_t T>
void ParallaxSerDes<T>::doneObject() {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::reset() {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(std::string& string) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(float& a) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(double& a) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(int& a) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(unsigned int& a) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(long& a) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(unsigned long& a) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(long long& a) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(unsigned long long& a) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(char& str) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(unsigned char& str) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(const std::string& str) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(float f) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(double d) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(int a) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(unsigned int a) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(long a) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(unsigned long a) {
    inner_dump(&a, sizeof(a));
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(long long a) {
    inner_dump(&a, sizeof(a));
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(unsigned long long a) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(char str) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(unsigned char str) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
void ParallaxSerDes<T>::push(const std::string& str1, const std::string& str2) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
std::string ParallaxSerDes<T>::get(const std::string& str1) {
    SERDES_FATAL("lala")
    return "";
}

template <std::size_t T>
void ParallaxSerDes<T>::pop(const std::string& str) {
    SERDES_FATAL("lala")
}

template <std::size_t T>
inline void ParallaxSerDes<T>::inner_dump(void* ptr, size_t size) {
    if (this->buffer_size_ + size > T) {
        std::cout << "Buffer too small" << std::endl;
        _exit(EXIT_FAILURE);
    }
    memcpy(buffer_.data() + this->buffer_size_, ptr, size);
    this->buffer_size_ += size;
}


}  // namespace fdb5
#endif
