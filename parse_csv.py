
import csv
from os import close

def parse_csv_files():
    # movies file
    line_cnt = 0
    in_f = open("IMDB movies.csv", encoding="cp437", mode="r")
    out_f = open("movies.csv", "w")
    csv_reader = csv.reader(in_f, delimiter=',')
    for row in csv_reader:
        if line_cnt == 1000:
            break
        else:
            try:
                genre = row[5].split(',')[0]
                out_f.write(row[0]+','+row[1].replace(',',' ')+','+row[3]+','+genre+','+row[6]+','+row[7].replace(',',' & ')+','+row[8].replace(',',' & ')+','+row[11].replace(',',' ')+','+row[12].replace(',',' & ')+','+row[13].replace(',',' ')+','+row[14]+','+row[15]+','+row[16].replace('$',' ')+'\n')
                line_cnt += 1
            except:
                print("problematic line")
    #close(in_f)
    #close(out_f)


    # actors file
    line_cnt = 0
    in_f = open("IMDB names.csv", encoding="cp437", mode="r")
    out_f = open("actors.csv", "w")
    csv_reader = csv.reader(in_f, delimiter=',')
    for row in csv_reader:
        if line_cnt == 1000:
            break
        else:
            try:
                out_f.write(row[0]+','+row[1]+','+row[2]+','+row[3]+','+row[4].replace(',',' ')+','+row[6].replace(',',' ')+','+row[7].replace(',',' ')+','+row[9]+','+row[10].replace(',',' ')+','+row[14]+','+row[16]+'\n')
                line_cnt += 1
            except:
                print("problematic line")
    #close(in_f)
    #close(out_f)


    # ratings file
    line_cnt = 0
    in_f = open("IMDB ratings.csv", encoding="cp437", mode="r")
    out_f = open("ratings.csv", "w")
    csv_reader = csv.reader(in_f, delimiter=',')
    for row in csv_reader:
        if line_cnt == 1000:
            break
        else:
            try:
                out_f.write(row[0]+','+row[1]+','+row[2]+','+row[3]+','+row[4]+','+row[15]+','+row[16]+','+row[17]+','+row[18]+','+row[19]+','+row[20]+','+row[21]+','+row[21]+','+row[22]+','+row[23]+','+row[24]+','+row[25]+','+row[26]+','+row[27]+','+row[28]+','+row[29]+','+row[30]+','+row[31]+','+row[32]+','+row[33]+','+row[34]+','+row[35]+','+row[36]+','+row[37]+','+row[38]+','+row[39]+','+row[40]+','+row[41]+','+row[42]+','+row[43]+','+row[44]+','+row[45]+'\n')
                line_cnt += 1
            except:
                print("problematic line")
    #close(in_f)
    #close(out_f)


parse_csv_files()
