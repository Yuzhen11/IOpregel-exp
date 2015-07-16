
#include "hdfs.h"
#include <string.h> //memcpy, memchr
#include <stdlib.h> //realloc
#include <vector>
#include <string>
#include <algorithm>
#include <iostream>
#include <sstream>
#include "utils/global.h"
using namespace std;

#define HDFS_BUF_SIZE 65536
#define LINE_DEFAULT_SIZE 4096
#define HDFS_BLOCK_SIZE 8388608 //8M

hdfsFS getlocalFS()
{
    hdfsFS lfs = hdfsConnect(NULL, 0);
    if (!lfs) {
        fprintf(stderr, "Failed to connect to 'local' FS!\n");
        exit(-1);
    }
    return lfs;
}

//====== get File Handle ======

hdfsFile getRHandle(const char* path, hdfsFS fs)
{
    hdfsFile hdl = hdfsOpenFile(fs, path, O_RDONLY | O_CREAT, 0, 0, 0);
    if (!hdl) {
        fprintf(stderr, "Failed to open %s for reading!\n", path);
        exit(-1);
    }
    return hdl;
}
hdfsFile getWHandle(const char* path, hdfsFS fs)
{
    hdfsFile hdl = hdfsOpenFile(fs, path, O_WRONLY | O_CREAT, 0, 0, 0);
    if (!hdl) {
        fprintf(stderr, "Failed to open %s for writing!\n", path);
        exit(-1);
    }
    return hdl;
}

//====== Read line ======

//logic:
//buf[] is for batch reading from HDFS file
//line[] is a line buffer, the string length is "length", the buffer size is "size"
//after each readLine(), need to check eof(), if it's true, no line is read due to EOF
struct LineReader {
    //static fields
    char buf[HDFS_BUF_SIZE];
    tSize bufPos;
    tSize bufSize;
    hdfsFS fs;
    hdfsFile handle;
    bool fileEnd;

    //dynamic fields
    char* line;
    int length;
    int size;

    LineReader(hdfsFS& fs, hdfsFile& handle)
        : bufPos(0)
        , length(0)
        , size(LINE_DEFAULT_SIZE)
    {
        this->fs = fs;
        this->handle = handle;
        fileEnd = false;
        fill();
        line = (char*)malloc(LINE_DEFAULT_SIZE * sizeof(char));
    }

    ~LineReader()
    {
        free(line);
    }

    //internal use only!
    void doubleLineBuf()
    {
        size *= 2;
        line = (char*)realloc(line, size * sizeof(char));
    }

    //internal use only!
    void lineAppend(char* first, int num)
    {
        while (length + num + 1 > size)
            doubleLineBuf();
        memcpy(line + length, first, num);
        length += num;
    }

    //internal use only!
    void fill()
    {
        bufSize = hdfsRead(fs, handle, buf, HDFS_BUF_SIZE);
        if (bufSize == -1) {
            fprintf(stderr, "Read Failure!\n");
            exit(-1);
        }
        bufPos = 0;
        if (bufSize < HDFS_BUF_SIZE)
            fileEnd = true;
    }

    //user interface
    //the line starts at "line", with "length" chars
    void readLine()
    {
        length = 0;
        if (bufPos == bufSize)
            return;
        char* pch = (char*)memchr(buf + bufPos, '\n', bufSize - bufPos);
        if (pch == NULL) {
            lineAppend(buf + bufPos, bufSize - bufPos);
            bufPos = bufSize;
            if (!fileEnd)
                fill();
            else
                return;
            pch = (char*)memchr(buf, '\n', bufSize);
            while (pch == NULL) {
                lineAppend(buf, bufSize);
                if (!fileEnd)
                    fill();
                else
                    return;
                pch = (char*)memchr(buf, '\n', bufSize);
            }
        }
        int validLen = pch - buf - bufPos;
        lineAppend(buf + bufPos, validLen);
        bufPos += validLen + 1; //+1 to skip '\n'
        if (bufPos == bufSize) {
            if (!fileEnd)
                fill();
            else
                return;
        }
    }

    char* getLine()
    {
        line[length] = '\0';
        return line;
    }

    bool eof()
    {
        return length == 0 && fileEnd;
    }
};


//====== Write line ======

const char* newLine = "\n";

struct LineWriter {
    hdfsFS fs;
    const char* path;
    int me; //-1 if there's no concept of machines (like: hadoop fs -put)
    int nxtPart;
    int curSize;

    hdfsFile curHdl;

    LineWriter(const char* path, hdfsFS fs, int me)
        : nxtPart(0)
        , curSize(0)
    {
        this->path = path;
        this->fs = fs;
        this->me = me;
        curHdl = NULL;
        //===============================
        //if(overwrite==true) readDirForce();
        //else readDirCheck();
        //===============================
        //1. cannot use above, otherwise multiple dir check/delete will be done during parallel writing
        //2. before calling the constructor, make sure "path" does not exist
        nextHdl();
    }

    ~LineWriter()
    {
        if (hdfsFlush(fs, curHdl)) {
            fprintf(stderr, "Failed to 'flush' %s\n", path);
            exit(-1);
        }
        hdfsCloseFile(fs, curHdl);
    }

    /*//================== not for parallel writing =====================
    //internal use only!
    void readDirCheck()
{
    	if(hdfsExists(fs, path)==0)
    	{
    		fprintf(stderr, "%s already exists!\n", path);
    		exit(-1);
    	}
    	int created=hdfsCreateDirectory(fs, path);
    	if(created==-1)
    	{
    		fprintf(stderr, "Failed to create folder %s!\n", path);
    		exit(-1);
    	}
}

    //internal use only!
    void readDirForce()
{
    	if(hdfsExists(fs, path)==0)
    	{
    		if(hdfsDelete(fs, path)==-1)
    		{
    			fprintf(stderr, "Error deleting %s!\n", path);
    			exit(-1);
    		}
    	}
    	int created=hdfsCreateDirectory(fs, path);
    	if(created==-1)
    	{
    		fprintf(stderr, "Failed to create folder %s!\n", path);
    		exit(-1);
    	}
}
    */ //================== not for parallel writing =====================

    //internal use only!
    void nextHdl()
    {
        //set fileName
        char fname[20];
        strcpy(fname, "part_");
        char buffer[10];
        if (me >= 0) {
            sprintf(buffer, "%d", me);
            strcat(fname, buffer);
            strcat(fname, "_");
        }
        sprintf(buffer, "%d", nxtPart);
        strcat(fname, buffer);
        //flush old file
        if (nxtPart > 0) {
            if (hdfsFlush(fs, curHdl)) {
                fprintf(stderr, "Failed to 'flush' %s\n", path);
                exit(-1);
            }
            hdfsCloseFile(fs, curHdl);
        }
        //open new file
        nxtPart++;
        curSize = 0;
        char* filePath = new char[strlen(path) + strlen(fname) + 2];
        strcpy(filePath, path);
        strcat(filePath, "/");
        strcat(filePath, fname);
        curHdl = getWHandle(filePath, fs);
        delete[] filePath;
    }

    void writeLine(char* line, int num)
    {
        if (curSize + num + 1 > HDFS_BLOCK_SIZE) //+1 because of '\n'
        {
            nextHdl();
        }
        tSize numWritten = hdfsWrite(fs, curHdl, line, num);
        if (numWritten == -1) {
            fprintf(stderr, "Failed to write file!\n");
            exit(-1);
        }
        curSize += numWritten;
        numWritten = hdfsWrite(fs, curHdl, newLine, 1);
        if (numWritten == -1) {
            fprintf(stderr, "Failed to create a new line!\n");
            exit(-1);
        }
        curSize += 1;
    }
};



//====== Put: local->HDFS ======

void put(char* localpath, char* hdfspath)
{
    hdfsFS lfs = getlocalFS();

    hdfsFile in = getRHandle(localpath, lfs);
    LineReader* reader = new LineReader(lfs, in);
    LineWriter* writer = new LineWriter(hdfspath, lfs, -1);
    while (true) {
        reader->readLine();
        if (!reader->eof()) {
            writer->writeLine(reader->line, reader->length);
        } else
            break;
    }
    hdfsCloseFile(lfs, in);
    delete reader;
    delete writer;

    hdfsDisconnect(lfs);
}

int main(int argc, char** argv)
{
    put(argv[1], argv[2]);
    return 0;
}

