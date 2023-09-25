#ifndef PARALLAXSERDES_H
#define PARALLAXSERDES_H
#include <string>
#include "eckit/persist/DumpLoad.h"

#define SERDES_DEBUG(...)                                                    \
    do {                                                                     \
        char buffer[1024];                                                   \
        snprintf(buffer, sizeof(buffer), __VA_ARGS__);                       \
        ::std::cout << __FILE__ << ":" << __func__ << ":" << __LINE__ << " " \
                    << " DEBUG: " << buffer << ::std::endl;                  \
    } while (0);

namespace fdb5 {


template <std::size_t T>
class ParallaxSerDes : public eckit::DumpLoad {
public:
    ParallaxSerDes() { buffer_.fill(0); }
    ~ParallaxSerDes() = default;

private:
    virtual void beginObject(const std::string& str);
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
    // members
    std::array<char, T> buffer_;
};

template <std::size_t T>
void ParallaxSerDes<T>::beginObject(const std::string& str) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::endObject() {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::nullObject() {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
std::string ParallaxSerDes<T>::nextObject() {
    SERDES_DEBUG("poutses")
    return "";
}

template <std::size_t T>
void ParallaxSerDes<T>::doneObject() {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::reset() {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(std::string& string) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(float& a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(double& a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(int& a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(unsigned int& a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(long& a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(unsigned long& a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(long long& a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(unsigned long long& a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(char& str) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::load(unsigned char& str) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(const std::string& str) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(float f) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(double d) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(int a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(unsigned int a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(long a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(unsigned long a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(long long a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(unsigned long long a) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(char str) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::dump(unsigned char str) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
void ParallaxSerDes<T>::push(const std::string& str1, const std::string& str2) {
    SERDES_DEBUG("poutses")
}

template <std::size_t T>
std::string ParallaxSerDes<T>::get(const std::string& str1) {
    SERDES_DEBUG("poutses")
    return "";
}

template <std::size_t T>
void ParallaxSerDes<T>::pop(const std::string& str) {
    SERDES_DEBUG("poutses")
}

}  // namespace fdb5
#endif