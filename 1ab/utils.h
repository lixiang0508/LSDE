#include <time.h>
#include <string.h>

#define REPORTING_N 1000000
#define LINEBUFLEN 1024

typedef unsigned long byteoffset;
typedef unsigned int  entrycount;
typedef unsigned char bool;

typedef struct
{
    unsigned long  person_id;
    unsigned short birthday;
    unsigned short know_each_other_first;
    unsigned short know_each_other_n;
    unsigned short interest_first;
    unsigned short interest_n;
}Person_Structure;

typedef struct {
	unsigned long  person_id;
	unsigned short birthday;
	unsigned short location;
	unsigned long  knows_first;
	unsigned short knows_n;
	unsigned long  interests_first;
	unsigned short interest_n;
} Person;

typedef struct {
    unsigned long  person_id;
    unsigned int  knows_first;
    unsigned short knows_n;
} Person_reduce;

typedef struct{
    unsigned short birthday;
    unsigned int person_offset;
} Person_birthday;

typedef struct{
    unsigned int person_offset;
    unsigned long knows_first;
    unsigned short knows_n;
} Person_buffer;

typedef struct {
    unsigned short interest_id;
    unsigned long  liked_Inverted_File_first;
    unsigned int  liked_Inverted_File_n;
} Artist;


typedef struct {
    unsigned long  person_id;
    unsigned long  knows_id;
    unsigned char  score;
} Result;



int person_birthday_comparator(const void* p1, const void* p2){
    Person_birthday* person1=(Person_birthday*) p1;
    Person_birthday* person2=(Person_birthday*) p2;
    if(person1->birthday<person2->birthday)
        return -1;
    else if(person1->birthday==person2->birthday)
        return 0;
    else
        return 1;
}

void parse_csv(char* fname, void (*line_handler)(unsigned char nfields, char** fieldvals)) {
	long nlines = 0;

   	FILE* stream = fopen(fname, "r");
	if (stream == NULL) {
		fprintf(stderr, "Can't read file at %s\n", fname);
		exit(-1);
	}
	char line[LINEBUFLEN];
	char* tokens[10]; //column pointers
	unsigned int col, idx;
	tokens[0] = line;

	while (fgets(line, LINEBUFLEN, stream)) {
		col = 0;
		// parse the csv line into array of strings
		for (idx=0; idx<LINEBUFLEN; idx++) {
			if (line[idx] == '|' || line[idx] == '\n') {
				line[idx] = '\0';
				col++;
				tokens[col] = &line[idx+1];
			} // lookahead to find end of line
			if (line[idx+1] == '\0') {
				break;
			}
		}
		(*line_handler)(col, tokens);
		nlines++;
		if (nlines % REPORTING_N == 0) {
			printf("%s: read %lu lines\n", fname, nlines);
		}
	}
	fclose(stream);
}

void print_person(Person *p){
printf("id: %li\nbirthday: %hi\nlocation: %hi\nknows_first: %li\nknows_n: %hi\n",p->person_id, p->birthday,p->location,p->knows_first, p->knows_n );
}

FILE* open_binout(char* filename) {
	FILE* outfile;
	outfile = fopen(filename, "wb");
	if (outfile == NULL) {
		fprintf(stderr, "Could not open %s for writing\n", filename);
		exit(-1);
	}
	return outfile;
}

/*
	convert birthday from date to four-digit integer with month and day
	easier comparisions and less storage space
*/
unsigned short birthday_to_short(char* date) {
	unsigned short bdaysht;
	char dmbuf[3];
	dmbuf[2] = '\0';
	dmbuf[0] = *(date + 5);
	dmbuf[1] = *(date + 6);
	bdaysht = atoi(dmbuf) * 100;
	dmbuf[0] = *(date + 8);
	dmbuf[1] = *(date + 9);
	bdaysht += atoi(dmbuf);
	return bdaysht;
}

void* mmapopen(char* filename, byteoffset *filelen, bool write) {
	int fd;
	struct stat sbuf;
	void *mapaddr;
	int fopenmode = write ? O_RDWR : O_RDONLY;
	int mmapmode = write ? PROT_WRITE : PROT_READ;

	if ((fd = open(filename, fopenmode)) == -1) {
		fprintf(stderr, "failed to open %s\n", filename);
		exit(1);
    }

    if (stat(filename, &sbuf) == -1) {
        fprintf(stderr, "failed to stat %s\n", filename);
        exit(1);
    }

    mapaddr = mmap(0, sbuf.st_size, mmapmode, MAP_SHARED, fd, 0);
    if (mapaddr == MAP_FAILED) {
        fprintf(stderr, "failed to mmap %s\n", filename);
        exit(1);
    }
    *filelen = sbuf.st_size;
    return mapaddr;
}

void* mmapr(char* filename, byteoffset *filelen) {
	return mmapopen(filename, filelen, 0);
}

void* mmaprw(char* filename, byteoffset *filelen) {
	return mmapopen(filename, filelen, 1);
}

char* makepath(char* dir, char* file, char* ext) {
	char* out = malloc(1024);
	sprintf(out, "%s/%s.%s", dir, file, ext);
	return out;
}


void timestamp()
{
    time_t ltime; /* calendar time */
    ltime=time(NULL); /* get current cal time */
    printf("%s",asctime( localtime(&ltime) ) );
}