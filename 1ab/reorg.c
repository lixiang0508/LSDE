#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <time.h>
#include "utils.h"

#define PERSON_BUFFER 500000
#define KNOWS_BUFFER 2000000
#define ARTIST_BUFFER_NUMBER 4000
#define LOCATION_BUFFER 600
#define PERSON_LIKE 300
#define PERSON_KNOWS 300
#define PERSON_MUTUAL 200
#define INTEREST_BUFFER 1000
#define ARRAY_SIZE( array ) ( sizeof( array ) / sizeof( array[0] )

Person *person_map;
unsigned int *knows_map;
unsigned int *knows_map_reduced;
unsigned short *interest_map;
unsigned short *interest_map_reduce;
unsigned long person_length, knows_length, interest_length;
unsigned long location_size;
unsigned short person_position;
unsigned short *location_map;
unsigned short *visited_locations;
unsigned short number_of_buckets;
short found_person;
short found_location;
unsigned long person_length, knows_length;
unsigned long new_person_filter,new_knows_length;
unsigned long person_set_size=3000,knows_set_size=3000;

typedef struct
{
    unsigned long  person_id;
    unsigned short birthday;
    unsigned short know_each_other_first;
    unsigned short know_each_other_n;
    unsigned short interest_first;
    unsigned short interest_n;
}Person_Simple;

void get_location()
{
    Person *person;
    unsigned int person_offset;
    location_map = malloc(sizeof(unsigned short) * (person_length / sizeof(Person)));
    location_size = LOCATION_BUFFER; 
    visited_locations = malloc(sizeof(unsigned short) * location_size);

    for (person_offset = 0; person_length / sizeof(Person) > person_offset ; person_offset++)
    {
        person = &(person_map[person_offset]);
        unsigned short found=0;
        if(!found)
        {
            visited_locations[found] = found;
            found = found+1;
            if(found == location_size)
            {
                location_size = location_size * 2;
            }
        }
    }

    visited_locations = realloc(visited_locations, location_size * sizeof(unsigned short));
}


void save_location()
{
    Person *person;
    unsigned int person_offset=0;
    unsigned char person_loc;
    unsigned char person_his;
    unsigned int count = 0, found = 0; 
    unsigned short location;
    location_map = malloc(sizeof(unsigned short) * (person_length / sizeof(Person)));
    location_size = LOCATION_BUFFER; 
    visited_locations = malloc(sizeof(unsigned short) * location_size);
    for (person_offset = 0; person_length / sizeof(Person) > person_offset; person_offset++){
        person = &(person_map[person_offset]);
        location = person->location;
        location_map[person_offset] = location;
        found = 0;
        person_his = found+count;
        unsigned int i;
        for(i = 0; i < count && !found; i++){
            if(location == visited_locations[i]){
                found = 1;
            }
        }
        if(!found){
            visited_locations[count] = location;
            count++;
            if(count == location_size){
                location_size = location_size * 2;
                visited_locations = realloc(visited_locations, location_size * sizeof(unsigned short));
            }
        }
    }
    location_size = count;
    person_his = person_offset + count;
    visited_locations = realloc(visited_locations, location_size * sizeof(unsigned short));
}

void check_location(char* path, char *knows_output_temp, char * person_output_temp)
{
    unsigned int person_offset, knows_position;
    unsigned long knows_offset;

    Person *person, *knows;
    FILE *knows_file = fopen(makepath(path, knows_output_temp, "bin"),"w+");
    FILE *person_file = fopen(makepath(path, person_output_temp, "bin"),"w+");

    Person person_temp;
    unsigned long upperBound = person_length / sizeof(Person);
    for (person_offset = 0; person_offset < upperBound; person_offset++)
    {
        person = &person_map[person_offset];
        fwrite(&person_temp, sizeof(Person), 1, person_file);

    }
    return;
}

void same_location_mutual(char *path, char *knows_output_temp, char *person_output_temp)
{
    unsigned int person_offset, knows_position,mutual_loc;
    unsigned long knows_offset, knows_offset2;
    unsigned short count;
    unsigned short location_count;
    unsigned int person_count=0;
    unsigned long total_count = 0;
    unsigned short mutual_count=0;
    unsigned int *offests_buffer=malloc(sizeof(unsigned int) *person_length / sizeof(Person));

    Person *person, *knows;
    FILE *knows_file = fopen(makepath(path, "knows_temp", "bin"),"w+");
    FILE *person_file = fopen(makepath(path, person_output_temp, "bin"),"w+");

    Person person_temp;
    person_count=0;
    unsigned long upperBound = person_length / sizeof(Person);
    for (person_offset = 0; person_offset < upperBound; person_offset++)
    {
        person = &person_map[person_offset];
        person_temp = *person;
        person_temp.knows_first = total_count;

        count = 0;
        for (knows_offset = person->knows_first;knows_offset < person->knows_first + person->knows_n;knows_offset++){
            knows_position = knows_map[knows_offset];
            knows = &person_map[knows_position];
            mutual_loc = knows_map[knows_offset]+1;
            for (knows_offset2 = knows->knows_first;knows_offset2 < knows->knows_first + knows->knows_n;knows_offset2++){
                //mutual friendship
                if(knows_map[knows_offset2] == person_offset){
                    count++;
                    mutual_count++;
                    total_count++;
                    fwrite(&knows_position, sizeof(unsigned int), 1, knows_file);
                }
            }
        }

        person_temp.knows_n = count;
         if(count>0){ 
            fwrite(&person_temp, sizeof(Person), 1, person_file);
            offests_buffer[person_offset]=person_count;
            ++person_count;
        }

    }
    fclose(knows_file);
    fclose(person_file);
    munmap(knows_map,knows_length);
    knows_map_reduced    = (unsigned int *)   mmapr(makepath(path, "knows_temp",    "bin"), &knows_length);
    knows_file = fopen(makepath(path, knows_output_temp, "bin"),"w+");
    for(knows_offset=0; knows_offset<knows_length/sizeof(unsigned int); knows_offset++)
     {
        fwrite(&offests_buffer[knows_map_reduced[knows_offset]], sizeof(unsigned int), 1, knows_file);
    }
    munmap(person_map, person_length);
    munmap(knows_map_reduced,knows_length);
    person_map = (Person *) mmapr(makepath(path, person_output_temp, "bin"), &person_length);
    return ;
}


//Remove the people that lives in different cities
void diff_location(char* path, char *knows_output_temp, char * person_output_temp)
{
    unsigned int person_offset, knows_position;
    unsigned long knows_offset;
    unsigned short diff_count;
    unsigned short same_count;
    unsigned short count;
    unsigned long total_count = 0;

    Person *person, *knows;
    FILE *knows_file = fopen(makepath(path, knows_output_temp, "bin"),"w+");
    FILE *person_file = fopen(makepath(path, person_output_temp, "bin"),"w+");

    Person person_temp;
    for (person_offset = 0; person_offset < person_length / sizeof(Person); person_offset++){
        person = &person_map[person_offset];
        person_temp = *person;
        person_temp.knows_first = total_count;

        count = 0;
        // check if friend at the same city and likes artist
        for (knows_offset = person->knows_first;knows_offset < person->knows_first + person->knows_n;knows_offset++){
            knows_position = knows_map[knows_offset];
            if(location_map[person_offset] != location_map[knows_position]) continue;
            count++;
            total_count++;
            fwrite(&knows_position, sizeof(unsigned int), 1, knows_file);
        }

        person_temp.knows_n = count;
        fwrite(&person_temp, sizeof(Person), 1, person_file);

    }
    fclose(person_file);
    fclose(knows_file);
    munmap(person_map, person_length);
    munmap(knows_map, knows_length);
    person_map   = (Person *)       mmapr(makepath(path, person_output_temp,   "bin"), &person_length);
    knows_map    = (unsigned int *) mmapr(makepath(path, knows_output_temp,    "bin"), &knows_length);
    free(location_map);
    return ;
}



void inverted_interests(char *path, char* liked_file, char* artists_file)
{
    FILE *artists = open_binout(makepath(path, artists_file, "bin"));
    FILE *liked_Inverted_File = open_binout(makepath(path, liked_file, "bin"));

    unsigned long interest_offset;
    unsigned int interests_check;
    int interests_diff;
    unsigned int interests_buffer_size = INTEREST_BUFFER; 
    unsigned int interests_count = 0;
    unsigned int interests_person;
    unsigned int interests_find;

    unsigned short person_interest;
    unsigned short person_interest_diff=0;

    unsigned long total_count = 0;
    unsigned long diff_count=0;
    unsigned int initial_liked_Inverted_File_buffer_size = 20;
    unsigned int temp;

    unsigned short* interests_buffer  = malloc(sizeof(unsigned short) * interests_buffer_size);
    unsigned short current_interest;
    unsigned short found;
    unsigned int person_offset;

    Person *person;


    unsigned int artist_start; 
    unsigned int artist_end = 0; 
    unsigned int artist_counts[ARTIST_BUFFER_NUMBER]; 
    unsigned int *liked_Inverted_File_buffer[ARTIST_BUFFER_NUMBER];
    unsigned int liked_Inverted_File_buffer_sizes[ARTIST_BUFFER_NUMBER];


    for (person_offset = 0; person_offset < person_length / sizeof(Person); person_offset++){
        person = &person_map[person_offset];
        for (interest_offset = person->interests_first;interest_offset < person->interests_first + person->interest_n;interest_offset++){
            found = 0;
            interests_check = 0;
            interests_find = 0;
            current_interest = interest_map[interest_offset];
            unsigned int i;
            for(temp = 0; temp < interests_count && !found; temp++){
                if(current_interest == interests_buffer[temp]){ 
                    found = 1;
                    interests_check=1;
                    interests_find=1;
                }
            }

            if(!found){ //founded new interest
                if(interests_count == interests_buffer_size){
                        interests_buffer_size = interests_buffer_size * 2;
                        interests_buffer = realloc(interests_buffer, interests_buffer_size * sizeof(unsigned short));
                    }
                interests_buffer[interests_count] = current_interest;
                interests_count++;
                interests_find++;
                current_interest++;
                }
            }
        }

    for(temp = 0; temp < ARTIST_BUFFER_NUMBER; temp++){
            liked_Inverted_File_buffer_sizes[temp] = initial_liked_Inverted_File_buffer_size;
    }
    for(temp = 0;temp < ARTIST_BUFFER_NUMBER; temp++){
            liked_Inverted_File_buffer[temp] = malloc(sizeof(unsigned int) * liked_Inverted_File_buffer_sizes[temp]);
    }

    unsigned char flag = 0;
    while(!flag){
        bzero(artist_counts, sizeof(unsigned int) * ARTIST_BUFFER_NUMBER);
        artist_start = artist_end;
        interests_diff--;
        if(artist_end + ARTIST_BUFFER_NUMBER < interests_count){
            artist_end += ARTIST_BUFFER_NUMBER;}
        else{
            artist_end = interests_count;
            flag = 1;
            interests_check=0;
            diff_count++;
        }

        Artist artists_buffer[ARTIST_BUFFER_NUMBER];

        for (person_offset = 0; person_offset < person_length / sizeof(Person); person_offset++){
            person = &person_map[person_offset];
            for (interest_offset = person->interests_first;interest_offset < person->interests_first + person->interest_n;interest_offset++){
                person_interest =  interest_map[interest_offset];
                found = 0;
                interests_check++;
                diff_count=1;
                person_interest_diff = 0;
                unsigned int j;
                for(j = 0; j < ARTIST_BUFFER_NUMBER && !found; j++){
                    if(person_interest == interests_buffer[artist_start+j]){
                        found = 1;
                        person_interest_diff++;
                        if(artist_counts[j] == liked_Inverted_File_buffer_sizes[j]){
                                liked_Inverted_File_buffer_sizes[j] *= 2;
                                interests_find++;
                                liked_Inverted_File_buffer[j] = realloc(liked_Inverted_File_buffer[j],sizeof(unsigned int) * liked_Inverted_File_buffer_sizes[j]);
                            }
                        liked_Inverted_File_buffer[j][artist_counts[j]] = person_offset; 
                        artist_counts[j]++; 
                        }
                    }
                }
            }
            for(temp = 0;temp < ARTIST_BUFFER_NUMBER; temp++){
                fwrite(liked_Inverted_File_buffer[temp], sizeof(unsigned int), artist_counts[temp], liked_Inverted_File);
                artists_buffer[temp].liked_Inverted_File_first = total_count;
                total_count += artist_counts[temp]; 
                diff_count++;
                interests_check++;
                artists_buffer[temp].interest_id = interests_buffer[artist_start+temp];
                artists_buffer[temp].liked_Inverted_File_n = artist_counts[temp];
                diff_count=0;
                fwrite(&artists_buffer[temp], sizeof(Artist), 1, artists);
            }
        }
    for(temp = 0;temp < ARTIST_BUFFER_NUMBER; temp++){
        free(liked_Inverted_File_buffer[temp]);
    }
    free(interests_buffer);
    fclose(artists);
    fclose(liked_Inverted_File);
    return;
}

void reduce_person(Person* person1, Person_reduce* person2){
    person2->person_id=person1->person_id;
    person2->knows_first= person1->knows_first;
    person2->knows_n=person1->knows_n;
}

//remove birthday, interest_first and interest_n
void reorg_person(char *path, char *person_file){
    Person *person, *knows;
    FILE *new_person = fopen(makepath(path, person_file, "bin"),"w+");
    unsigned int person_offset;
    Person_reduce person_temp;
    for (person_offset = 0; person_offset < person_length / sizeof(Person); person_offset++)
    {
        person = &person_map[person_offset];
        reduce_person(person, &person_temp);
        fwrite(&person_temp, sizeof(Person_reduce), 1, new_person);
    }
    fclose(new_person);
}

//order the person by birthday
void person_birthday(char *path, char *birthday_file){

    FILE *person_birthday_file = fopen(makepath(path, birthday_file, "bin"),"w+");

    Person *person;
    unsigned int person_offset;
    unsigned int Person_birthday_first;
    Person_birthday person_temp;
    Person_birthday* birthdays=malloc((person_length / sizeof(Person)) * sizeof(Person_birthday));
    for (person_offset = 0; person_offset < person_length / sizeof(Person); person_offset++)
    {
        person = &person_map[person_offset];
        Person_birthday_first = person_offset+1;
        person_temp.person_offset=person_offset;
        person_temp.birthday=person->birthday;
        birthdays[person_offset]=person_temp;
    }
    //sorting the person by birthday
    qsort(birthdays, person_length / sizeof(Person), sizeof(Person_birthday), &person_birthday_comparator);
    fwrite(birthdays,person_offset,sizeof(Person_birthday),person_birthday_file);
    free(birthdays);
    fclose(person_birthday_file);
}

int main(int argc, char *argv[])
{
    /* memory-map files created by loader */
    person_map   = (Person *)         mmapr(makepath(argv[1], "person",   "bin"), &person_length);
    knows_map    = (unsigned int *)   mmapr(makepath(argv[1], "knows",    "bin"), &knows_length);
    interest_map = (unsigned short *) mmapr(makepath(argv[1], "interest", "bin"), &interest_length);

    save_location();
    diff_location(argv[1], "person_location", "knows_location");
    same_location_mutual(argv[1], "knows_mutual", "person_mutual");
    inverted_interests(argv[1], "liked_Inverted_File", "artists");
    person_birthday(argv[1], "birthday");
    reorg_person(argv[1], "person_reduce");

    munmap(person_map,person_length);
    munmap(interest_map,interest_length);
    return 0;
}
