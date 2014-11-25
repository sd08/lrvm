/*

CS6210 Project 4: Recoverable Virtual Memory
Georgia Institute of Technology

*/

#include "rvm.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <dirent.h>
#include <string>
#include <fstream>
#include <iostream>

using namespace std;

const char * _rvm_context::get_segment_name(void * segbase)
{
	map<void *, const char *>::const_iterator iter = mapped_segments.find(segbase);
	if(iter == mapped_segments.end())
	{
		return NULL;
	}
	else
	{
		return iter->second;
	}
}

bool _rvm_context::is_mapped(const char *segname)
{
	return (segment_names.find(segname) != segment_names.end());
}

void * _rvm_context::get_segment_ptr(const char * segname)
{
	string name = segname;
	for(map<void *, const char * >::iterator iter = mapped_segments.begin(); iter != mapped_segments.end(); ++iter)
	{
		if(name.compare(iter->second)==0)
		{
			return iter->first;
		}
	}
	return NULL;
}

int _rvm_context::get_segment_size(const char * segname)
{
	return is_mapped(segname)?segment_names.find(segname)->second:-1;
}
rvm_t rvm_init(const char *directory)
{
	if(directory == NULL)
	{
		return (rvm_t)-1;
	}
	_rvm_context *ctxt = new _rvm_context(directory);
	if(ctxt == NULL)
	{
		return (rvm_t) -1;
	}
	int dirfd = mkdir(directory,S_IRWXU);
	if(dirfd == -1 && errno != EEXIST)
	{
		delete ctxt;
		ctxt = (_rvm_context*) -1;
	}
	return (rvm_t) ctxt;
}

/*
  If rvm_map() is called with size_to_create of 0, and segname exists,
  the segment will be loaded with the previous segment size as
  determined by the backing file size.

0) Check parameters for garbage. Return an error if they stink.
1) Check to see if this segname is already mapped. return an error if this is so.
2) Find the size of the existing file on disk and grow it if its size is < size_to_create
3) Read the segment into a buffer.
4) If a redo log exists, play through the redo log into this buffer.
5) If a redo log exists, write the updated data segment to disk.
6) If a redo log exists, truncate the redo log.
7) Save this segment's information into our context.
*/


void rvm_apply_log(void *addr, int size, const char *log_file_name, fstream &segment)
{
	fstream log_file(log_file_name,ios_base::in|ios_base::out|ios_base::binary);

	if(log_file)
	{
		size_t offset;
		size_t length;
		bool log_applied = false;
	
		log_file.seekg(0,ios_base::beg);
	
		while(log_file.read((char *)&offset,sizeof(size_t)))
		{
			log_applied = true;
			log_file.read((char *)&length,sizeof(size_t));
			log_file.read((char *)addr + offset,length);
		}

		log_file.close();

		if(log_applied)
		{
// Step 5
			segment.seekp(0,ios_base::beg);
			segment.write((char *)addr,size);
// Step 6
			log_file.open(log_file_name,ios_base::in|ios_base::out|ios_base::binary|ios_base::trunc);
			log_file.close();
		}
	}
}


void *rvm_map(rvm_t rvm, const char *segname, int size_to_create)
{
// Step 0
	if(rvm == (rvm_t)-1 || rvm == (rvm_t)NULL || segname == NULL || size_to_create < 0)
	{
		return (void *)-1;
	}

	_rvm_context *ctxt = (_rvm_context *) rvm;
	
	string backing_file = ctxt->directory_store + "/" + segname + ".rvm";

// Step 1	
	if(ctxt->is_mapped(segname))
	{
		return (void *)-1;
	}

// Step 2
	fstream segment(backing_file.c_str(),fstream::binary | fstream::in | fstream::out);

	if(!segment)
	{
		segment.open(backing_file.c_str(),fstream::binary | fstream::in | fstream::out | fstream::trunc);
		if(!segment)
		{
			return (void*)-1;
		}
	}
	
	segment.seekg(0,ios_base::end);

	int filesize = segment.tellg();

	if( filesize == -1 )
	{
		segment.close();
		return (void *)-1;
	}

	if(size_to_create != 0 && size_to_create > filesize)
	{
		if(!(segment.seekp(size_to_create-1,ios_base::beg)))
		{
			segment.close();
			return (void *)-1;
		}
		char zero = '\0';
		if(!(segment.write(&zero,sizeof(char))))
		{
			segment.close();
			return (void *)-1;
		}
		
		if(!(segment.flush()))
		{
			segment.close();
			return (void *)-1;
		}
	}
	else
	{
		size_to_create = filesize;
	}

	if(!(segment.seekg(0,ios_base::beg)))
	{
		segment.close();
		return (void *)-1;
	}

// Step 3
	char * addr = new char[size_to_create];

	if(addr == NULL)
	{
		segment.close();
		return (void *)-1;
	}

	if(!(segment.read((char *)addr,size_to_create)))
	{
		segment.close();
		delete [] addr;
		return (void *)-1;
	}

// Step 4
	string log_file_name = backing_file;

	log_file_name.append("log");

	rvm_apply_log(addr,size_to_create,log_file_name.c_str(),segment);

	segment.close();

// Step 7
	const char *nameptr = (ctxt->segment_names.insert(make_pair(segname,size_to_create))).first->first.c_str();

	ctxt->mapped_segments.insert(make_pair(addr,nameptr));

	return addr;
}

void rvm_unmap(rvm_t rvm, void *segbase)
{
	if(rvm == (rvm_t)-1 || rvm == (rvm_t)NULL)
	{
		return; //Again, it is an error to pass a bad rvm, but we can't handle it due to this function's return type!!!
	}
	_rvm_context *ctxt = (_rvm_context *) rvm;

	delete [] (char *) segbase;

	ctxt->segment_names.erase(ctxt->get_segment_name(segbase));
	ctxt->mapped_segments.erase(segbase);
}

void rvm_destroy(rvm_t rvm, const char *segname)
{
	if(rvm == (rvm_t)-1 || rvm == (rvm_t)NULL)
	{
		return; //Again, it is an error to pass a bad rvm, but we can't handle it due to this function's return type!!!
	}
	_rvm_context *ctxt = (_rvm_context *) rvm;

	if(ctxt->is_mapped(segname))
	{
		return; //this segment is already mapped.  do nothing.
	}

	string backing_file = ctxt->directory_store + "/" + segname + ".rvm";
	
	unlink(backing_file.c_str());
	backing_file.append("log");
	unlink(backing_file.c_str());
}

trans_t rvm_begin_trans(rvm_t rvm, int numsegs, void **segbases)
{
	/*
	1) Create a new transaction context.
	2) For each entry in segbases, do:
		a) if the segment does not exist, return -1
		b) if the segment is being staged, return -1
		c) try to open the log file for this segment, if that fails, return -1
		d) Enter this segment into staging

	*/

	if(rvm == (void*)-1 || rvm == (void*)NULL || numsegs <= 0 || segbases == NULL)
	{
		return (trans_t)-1;
	}

	_rvm_context *ctxt = (_rvm_context *) rvm;
	_trans_context *trans = new _trans_context(ctxt);
	if(trans == NULL)
	{
		return (trans_t)-1;
	}

	for(int i = 0; i < numsegs; ++i)
	{
		map<void *, const char * >::iterator iter = ctxt->mapped_segments.find(segbases[i]);

		if( iter == ctxt->mapped_segments.end())
		{
			delete trans;
			return (trans_t)-1;
		}

		if( ctxt->staging.find(segbases[i]) != ctxt->staging.end())
		{
			delete trans;
			return (trans_t)-1;
		}

		string logfilename = ctxt->directory_store + "/" + iter->second + ".rvmlog";

		list<_rvm_change> emptychange;

		trans->logs.insert(make_pair(segbases[i],emptychange));

		ctxt->staging.insert(segbases[i]);
	}
	return trans;
}

void rvm_about_to_modify(trans_t tid, void *segbase, int offset, int size)
{
	if(tid == (trans_t) -1 || offset < 0 || size < 0)
	{
		return;
	}
	_trans_context *trans = (_trans_context *)tid;
	map<void *, list<_rvm_change> >::iterator iter = trans->logs.find(segbase);
	if(iter == trans->logs.end())
	{
		return;
	}
	_rvm_change change(offset,size,(char*)segbase);
	iter->second.push_back(change);
}


/*
	iterate over each segment
		iterate over each change
			write redo log for change
	delete undo records
*/
void rvm_commit_trans(trans_t tid)
{
	if(tid == (trans_t) -1 || tid == (trans_t) NULL)
	{
		return;
	}

	_trans_context *trans = (_trans_context*) tid;
	_rvm_context *ctxt = trans->parent_context;

	for(map<void *, list<_rvm_change> >::iterator segment = trans->logs.begin(); segment != trans->logs.end(); ++segment)
	{
		string seg_file_name = ctxt->directory_store + "/" + ctxt->get_segment_name(segment->first) + ".rvm";
		string log_file_name = seg_file_name;
		log_file_name.append("log");

		ofstream redolog(log_file_name.c_str(),ios_base::out|ios_base::binary|ios_base::trunc);
		/*
		If we can't open redolog here we're in trouble, but we can't recover from such a failure because we can't modify the API.
		*/
		for(list<_rvm_change>::iterator change = segment->second.begin(); change != segment->second.end(); ++change)
		{
			size_t len = change->data.length();
			redolog.write((char *)&(change->offset),sizeof(size_t));
			redolog.write((char *)&len,sizeof(size_t));
			redolog.write((char *)segment->first + change->offset,len);
		}

		redolog.close();
		ctxt->staging.erase(segment->first);	
	}
	delete trans;
}

void rvm_abort_trans(trans_t tid)
{
	if(tid == (trans_t) -1 || tid == (trans_t) NULL)
	{
		return;
	}

	_trans_context *trans = (_trans_context *) tid;
	_rvm_context *ctxt = trans->parent_context;

	for(map<void *, list<_rvm_change> >::iterator segment = trans->logs.begin(); segment != trans->logs.end(); ++segment)
	{
		for(list<_rvm_change>::reverse_iterator change = segment->second.rbegin(); change != segment->second.rend(); ++change)
		{
			memcpy((char *)segment->first + change->offset, change->data.c_str(), change->data.length());
		}
		ctxt->staging.erase(segment->first);
	}
	delete trans;
}

void rvm_truncate_log(rvm_t rvm)
{
	if((rvm == (rvm_t) -1) || (rvm == (rvm_t) NULL))
	{
		return;
	}
	
	DIR *d;
	dirent *dir;
	
	_rvm_context *ctxt = (_rvm_context *) rvm;

	d = opendir(ctxt->directory_store.c_str());

	if(d != NULL)
	{
		while((dir = readdir(d))!= NULL)
		{
			string file = dir->d_name;
#ifdef DEBUG
			cout<<"Truncate examine "<<file<<endl;
#endif
			size_t ext = file.rfind(".rvmlog");
			if(ext != string::npos)
			{
				string segname = file.substr(0,ext);
				string log_file_name = ctxt->directory_store + "/" + file;
#ifdef DEBUG
				cout<<"Truncating segment "<<segname<<endl;
#endif
				if(ctxt->is_mapped(segname.c_str()))
				{
					void *addr = ctxt->get_segment_ptr(segname.c_str());
					int size = ctxt->get_segment_size(segname.c_str());
					string segfilename = ctxt->directory_store + "/" + segname + ".rvm";
					fstream segment(segfilename.c_str(),fstream::in|fstream::out|fstream::binary|fstream::trunc);
					rvm_apply_log(addr,size,log_file_name.c_str(),segment);
					segment.close();
				}
				else
				{
					void *addr = rvm_map(rvm, segname.c_str() ,0);
					if(addr != (void *) -1)
					{
						rvm_unmap(rvm,addr);
					}
				}
#ifdef DEBUG
				cout<<"Unlinking "<<log_file_name<<endl;
#endif
				unlink(log_file_name.c_str());
#ifdef DEBUG
				cout<<"Unlink Errno: "<<strerror(errno)<<endl;
#endif
			}
		}
	}	
}
