#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "utils.h"

#define QUERY_FIELD_QID 0
#define QUERY_FIELD_A1 1
#define QUERY_FIELD_A2 2
#define QUERY_FIELD_A3 3
#define QUERY_FIELD_A4 4
#define QUERY_FIELD_BS 5
#define QUERY_FIELD_BE 6
#define PERSON_BUFFER 600000
#define PERSON_LIKE 300
#define PERSON_KNOWS 300
#define PERSON_MUTUAL 200
#define LOCATION_BUFFER 600
#define INTEREST_BUFFER 1000

Person_reduce *person_map;
Person_birthday *person_birthday_map;
Artist *artists_map;
unsigned int *knows_map;
unsigned char *scores_map; 
unsigned short *interest_map;
char *path;
unsigned int *liked_map;
unsigned long person_length, knows_length, score_length, artists_length, liked_length, person_birthday_length;
FILE *outfile;

//Binary search
int binsearch(int start, int end, short value)
{
    int middle;
    while (end > start)
    {
        middle = start + (end - start) / 2;

        if(person_birthday_map[middle].birthday < value)
        {
            start = middle + 1; 
        }
        else if(person_birthday_map[middle].birthday > value)
        {
            end = middle; 
        }
        else
        {
            while(middle >= start && person_birthday_map[middle].birthday == value)
            {
                --middle;
            }
            if(middle < start)
            {
                return start;
            }
            else
            {
                return middle + 1;
            }
        }
    }
    return start + 1;
}


int result_comparator(const void *v1, const void *v2)
{
    Result *r1 = (Result *) v1;
    Result *r2 = (Result *) v2;
    if (r1->score > r2->score)
        return -1;
    else if (r1->score < r2->score)
        return +1;
    else if (r1->person_id < r2->person_id)
        return -1;
    else if (r1->person_id > r2->person_id)
        return +1;
    else if (r1->knows_id < r2->knows_id)
        return -1;
    else if (r1->knows_id > r2->knows_id)
        return +1;
    else
        return 0;
}

unsigned char get_score(Person *person, unsigned short areltd[]) {
    long interest_offset;
    unsigned short interest;
    unsigned char score = 0;
    for (interest_offset = person->interests_first; 
        interest_offset < person->interests_first + person->interest_n; 
        interest_offset++) {

        interest = interest_map[interest_offset];
        if (areltd[0] == interest) score++;
        if (areltd[1] == interest) score++;
        if (areltd[2] == interest) score++;
        // early exit
        if (score > 2) {
            break;
        }
    }
    return score;
}

typedef struct
{
    unsigned long  person_id;
    unsigned short birthday;
    unsigned short know_each_other_first;
    unsigned short know_each_other_n;
    unsigned short interest_first;
    unsigned short interest_n;
}Person_Simple;

char likes_artist(Person *person, unsigned short artist) {
    long interest_offset;
    unsigned short interest;
    unsigned short likesartist = 0;

    for (interest_offset = person->interests_first; 
        interest_offset < person->interests_first + person->interest_n; 
        interest_offset++) {

        interest = interest_map[interest_offset];
        if (interest == artist) {
            likesartist = 1;
            break;
        }
    }
    return likesartist;
}

void count_score(unsigned short artist_id, unsigned short areltd[])
{
    unsigned int artist_offset; //offset in the artist_map
    unsigned long liked_offset; //offset in the liked_Inverted_File_map
    Artist *artist;

    //if the score = 4, the person likes A1
    scores_map = malloc(sizeof(unsigned char) * (person_length / sizeof(Person_reduce)));
    bzero(scores_map, sizeof(unsigned char) * (person_length / sizeof(Person_reduce)));

    for (artist_offset = 0;
            artist_offset < artists_length / sizeof(Artist);
            artist_offset++)
    {

        artist = &artists_map[artist_offset];
        if(artist->interest_id == artist_id) 
        {
            //value = 4 to the people who like A1
            for(liked_offset = artist->liked_Inverted_File_first;
                    liked_offset < artist->liked_Inverted_File_first + artist->liked_Inverted_File_n;
                    liked_offset++)
            {
                scores_map[liked_map[liked_offset]] = 4;
            }
        }

        if(areltd[0] == artist->interest_id || areltd[1] == artist->interest_id || areltd[2] == artist->interest_id)
        {
            for(liked_offset = artist->liked_Inverted_File_first;
                    liked_offset < artist->liked_Inverted_File_first + artist->liked_Inverted_File_n;
                    liked_offset++)
            {
                if(scores_map[liked_map[liked_offset]] != 4)
                    scores_map[liked_map[liked_offset]]++;
            }
        }
    }
}

void query(unsigned short qid, unsigned short artist, unsigned short areltd[], unsigned short bdstart, unsigned short bdend)
{
    unsigned int person_offset, knows_position, offset; //offsets in person_map
    unsigned long knows_offset, knows_offset2; //offsets in knows_map
    unsigned short person_reduce1,person_reduce2;
    Person_reduce *person, *knows;
    unsigned char score;
    unsigned char temp_score;
    unsigned int result_length = 0, result_idx, result_set_size = 1000;
    Result *results = malloc(result_set_size * sizeof (Result));
    Person_birthday pbday;
    pbday.birthday = bdstart;
    unsigned int person_num = person_birthday_length / sizeof(Person_birthday);
    unsigned int start_pos = binsearch(0, person_num, bdstart);

    for(offset = start_pos; offset < person_num && person_birthday_map[offset].birthday <= bdend; offset++)
    {

        score = scores_map[person_birthday_map[offset].person_offset];
        if (score == 0 || score == 4) continue; //score = 4 means that person likes A1

        person = &person_map[person_birthday_map[offset].person_offset];

        for (knows_offset = person->knows_first;
                knows_offset < person->knows_first + person->knows_n;
                knows_offset++)
        {
            knows_position = knows_map[knows_offset];
            knows = &person_map[knows_position];

            // check if friend likes A1
            if (scores_map[knows_position] != 4) continue;
            if (result_length >= result_set_size)
            {
                result_set_size *= 2;
                results = realloc(results, result_set_size * sizeof (Result));
            }
            results[result_length].person_id = person->person_id;
            results[result_length].knows_id = knows->person_id;
            results[result_length].score = score;
            result_length++;
        }
    }


    // sort result
    qsort(results, result_length, sizeof(Result), &result_comparator);

    // output
    for (result_idx = 0; result_idx < result_length; result_idx++)
    {
        fprintf(outfile, "%d|%d|%lu|%lu\n", qid, results[result_idx].score,
                results[result_idx].person_id, results[result_idx].knows_id);
    }

}

void query_line_handler(unsigned char nfields, char **tokens)
{
    unsigned short q_id, q_artist, q_bdaystart, q_bdayend;
    unsigned short q_relartists[3];

    q_id            = atoi(tokens[QUERY_FIELD_QID]);
    q_artist        = atoi(tokens[QUERY_FIELD_A1]);
    q_relartists[0] = atoi(tokens[QUERY_FIELD_A2]);
    q_relartists[1] = atoi(tokens[QUERY_FIELD_A3]);
    q_relartists[2] = atoi(tokens[QUERY_FIELD_A4]);
    q_bdaystart     = birthday_to_short(tokens[QUERY_FIELD_BS]);
    q_bdayend       = birthday_to_short(tokens[QUERY_FIELD_BE]);

    artists_map = (Artist *)  mmapr(makepath(path, "artists",    "bin"), &artists_length);
    liked_map = (unsigned int *)  mmapr(makepath(path, "liked_Inverted_File",    "bin"), &liked_length);

    count_score(q_artist, q_relartists);

    munmap(artists_map, artists_length);
    munmap(liked_map, liked_length);

    query(q_id, q_artist, q_relartists, q_bdaystart, q_bdayend);

}



int main(int argc, char *argv[])
{

    if (argc < 4)
    {
        fprintf(stderr, "Usage: [datadir] [query file] [results file]\n");
        exit(1);
    }
    path=argv[1];

    person_map   = (Person_reduce *)    mmapr(makepath(argv[1], "person_reduce",   "bin"), &person_length);
    knows_map    = (unsigned int *)   mmapr(makepath(argv[1], "knows_mutual",    "bin"), &knows_length);
    person_birthday_map = (Person_birthday *) mmapr(makepath(argv[1], "birthday", "bin"), &person_birthday_length);

    outfile = fopen(argv[3], "w");
    if (outfile == NULL)
    {
        fprintf(stderr, "Can't write to output file at %s\n", argv[3]);
        exit(-1);
    }

    parse_csv(argv[2], &query_line_handler);
    
    return 0;
}
