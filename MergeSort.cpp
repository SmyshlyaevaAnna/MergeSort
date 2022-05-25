#define _CRT_SECURE_NO_WARNINGS
#include <iostream>
#include <cstdlib>
#include <cmath>
#include <mutex>
#include <vector>
#include <algorithm>
#include <string>
#include <iterator>
#include <chrono>
#include <thread>
#include <iostream>
#include <queue>
#include <ctime>


using namespace std;

FILE* f;
bool mark = false;
mutex file_mutex;


class SafeQueue 
{
private:
	queue <string> A;
	mutex mutex_A;
	mutex mutex_count;
	mutex cut_mutex;
	int cut_count = 0;
public:
	int counter_thread = 0;

	int push(string val)
	{
		lock_guard <mutex> g(mutex_A);
		A.push(val);
		return 1;
	}	
	int size() //Возвращает количество файлов в очереди
	{
		return A.size();
	}
	size_t get_counter_thread() //Возвращает общее число потоков
	{
		return counter_thread;
	}
	void thread_dec()
	{
		lock_guard <mutex> test(mutex_count);
		counter_thread--;
	}
	int pair_pop(string& a, string& b)
	{
		lock_guard <mutex> test(mutex_A);
		switch (A.size())
		{
		case 0:
			return 0;
		case 1:
			a = A.front();
			A.pop();
			return 1;
		default:
			a = A.front();
			A.pop();
			b = A.front();
			A.pop();
			return 2;
		}
	}
};

//делим файлы и сортируем внутри файла
void AddThread(SafeQueue& files)

{
	int l = 0;
	while (!mark)
	{
		l++;
		FILE* f_buf;
		//int N = round((100 * pow(2, 20)) / (sizeof(int)));
		int N = 100;
		vector <int> A(N);
		file_mutex.lock();
		int real_size = fread(A.data(), sizeof(int), N, f);
		file_mutex.unlock();

		if (real_size == N)
		{
			f_buf = fopen(string("data" + to_string(l + hash<thread::id>{}(this_thread::get_id())) + ".bin").c_str(), "wb+");
			sort(A.begin(), A.end());
			fwrite(A.data(), sizeof(int), real_size, f_buf);
			fclose(f_buf);
			files.push("data" + to_string(l + hash<thread::id>{}(this_thread::get_id())) + ".bin");
		}

		if ((real_size < N) && (real_size > 0))
		{
			f_buf = fopen(string("data" + to_string(l + hash<thread::id>{}(this_thread::get_id())) + ".bin").c_str(), "wb+");
			sort(A.begin(), A.end());
			fwrite(A.data(), sizeof(int), real_size, f_buf);
			fclose(f_buf);
			files.push("data" + to_string(l + hash<thread::id>{}(this_thread::get_id())) + ".bin");
			mark = true;
		}
		if (real_size == 0)
		{
			mark = true;
		}
	}			
	

}

void merge_file(SafeQueue& base, string name1, string name2)
{
	FILE* f1 = fopen(name1.c_str(), "rb+");
	FILE* f2 = fopen(name2.c_str(), "rb+");
	FILE* res = fopen((name1+name2).c_str(), "wb+");
	int i1 = 0, i2 = 0, s1, s2;
	s1 = fread(&i1, sizeof(int), 1, f1);
	s2 = fread(&i2, sizeof(int), 1, f2);
	while (s1 * s2 > 0)
	{
		if (i2 > i1)
		{
			fseek(f2, -int(sizeof(int)), SEEK_CUR);
			fwrite(&i1, sizeof(int), 1, res);
			s1 = fread(&i1, sizeof(int), 1, f1);
		}
		else
		{
			fseek(f1, -int(sizeof(int)), SEEK_CUR);
			fwrite(&i2, sizeof(int), 1, res);
			s2 = fread(&i2, sizeof(int), 1, f1);
		}
	}
	if (s1 == 0)
	{
		while (s2 > 0)
		{
			fwrite(&i2, sizeof(int), 1, res);
			s2 = fread(&i2, sizeof(int), 1, f1);
		}
	}
	if (s2 == 0)
	{
		while (s1 > 0)
		{
			fwrite(&i1, sizeof(int), 1, res);
			s1 = fread(&i1, sizeof(int), 1, f1);
		}
	}
	fclose(res);
	fclose(f1);
	fclose(f2);
	remove(name1.c_str());
	remove(name2.c_str());
	base.push(name1 + name2);
	base.thread_dec();
}




int main()
{
	f = fopen("test.bin", "rb+"); 
	SafeQueue temp_files;
	string buf1, buf2;
	vector <thread> merge_threads;
	vector <thread> threads(100);
	for (int k = 0; k < 4; k++)
		threads[k] = thread(AddThread, ref(temp_files));
	for (int k = 0; k < 4; k++)
		threads[k].join();
	while (true)
	{
		if ((temp_files.size() == 1) && (temp_files.get_counter_thread() == 0))
			break;
		string buf1, buf2;
		temp_files.pair_pop(buf1, buf2);
		if (temp_files.get_counter_thread() < thread::hardware_concurrency())
			merge_threads.push_back(thread(merge_file, ref(temp_files), buf1, buf2));
		else this_thread::sleep_for(5s);
	}
	for (auto& x : merge_threads)
		if (x.joinable())
			x.join();
	for (auto& x : threads)
		if (x.joinable())
			x.join();
	cout << buf1 << "- File_counter- " << temp_files.size();

}
