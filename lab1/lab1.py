import psycopg2
import datetime


def connect_to_db():
    conn = psycopg2.connect(
        host='localhost', port='5432', database='lab1',
        user='user1', password='user1')

    return conn


def insert_artist(artist=None):
    if artist is None:
        artist = input("Print artist name\n")
    fans = int(input("Print number of funs\n"))
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("SELECT max(id) FROM artist")
    id = int(cur.fetchone()[0]) + 1
    cur.execute("INSERT INTO artist(id,name,fans) VALUES (%s,%s,%s)",
                (id, artist, fans))
    conn.commit()
    cur.close()
    conn.close()


def insert_album(album=None, artist=None):
    if album is None:
        album = input("Print album name\n")
    rait = float(input("Print album raiting\n"))
    date_str = input("Print date (YYYY-MM-DD)\n")
    release = parse_date(date_str)
    conn = connect_to_db()
    cur = conn.cursor()
    if artist is None:
        artist = input("Print artist\n")
    cur.execute("SELECT id FROM artist WHERE name = '%s' " % (artist))
    if cur.rowcount == 0:
        insert_artist(artist)
    cur.execute("SELECT max(id) FROM artist")
    artist_id = int(cur.fetchone()[0])
    cur.execute("SELECT max(id) FROM album")
    id = int(cur.fetchone()[0]) + 1
    cur.execute("INSERT INTO album(id,artist_id,title,raiting,release) VALUES (%s,%s,%s,%s,%s)",
                (id, artist_id, album, rait, release))
    conn.commit()
    cur.close()
    conn.close()


def insert_song():
    song = input("Print song name\n")
    artist = input("Print artist\n")
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM artist WHERE name = '%s' " % (artist))
    if cur.rowcount == 0:
        insert_artist(artist)
    single_str = input("Is it single?(yes/no)\n")
    single = False

    if single_str == "yes":
        single = True

    if not single:
        album = input("Print album name\n")
        cur.execute("SELECT id FROM album WHERE title = '%s' " % (album))
        if cur.rowcount == 0:
            insert_album(album, artist)

    cur.execute("SELECT max(id) FROM artist")
    artist_id = int(cur.fetchone()[0])
    cur.execute("SELECT max(id) FROM song")
    song_id = int(cur.fetchone()[0]) + 1
    cur.execute("SELECT max(id) FROM album")
    album_id = int(cur.fetchone()[0])
    cur.execute("INSERT INTO song(id,artist_id,name,single) VALUES (%s,%s,%s,%s) ",
                (song_id, artist_id, song, single))
    conn.commit()
    cur.execute("INSERT INTO song_album(song_id,album_id) VALUES (%s,%s) ",
                (song_id, album_id))
    conn.commit()
    conn.close()


def delete_song(song=None):
    if song is None:
        song = input("Print song name\n")
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM song WHERE name = '%s' " % (song))
    if cur.rowcount == 0:
        return
    song_id = int(cur.fetchone()[0])
    cur.execute("DELETE FROM song_album WHERE song_id = '%s' " % (song_id))
    conn.commit()
    cur.execute("DELETE FROM song WHERE id = '%s' " % (song_id))
    conn.commit()
    cur.close()
    conn.close()


def delete_album(album=None):
    if album is None:
        album = input("Print album name\n")
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM album WHERE title = '%s' " % (album))
    if cur.rowcount == 0:
        return
    album_id = int(cur.fetchone()[0])
    cur.execute("SELECT song_id FROM song_album WHERE album_id = %s" % (album_id))
    for row in cur.fetchall():
        cur.execute("DELETE FROM song_album WHERE song_id = '%s' " % (row[0]))
        conn.commit()
        cur.execute("DELETE FROM song WHERE id = '%s' " % (row[0]))
        conn.commit()

    cur.execute("DELETE FROM album WHERE id = '%s' " % (album_id))
    conn.commit()
    cur.close()
    conn.close()


def delete_artist():
    artist = input("Print atrist name\n")
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM artist WHERE name = '%s' " % (artist))
    if cur.rowcount == 0:
        return
    artist_id = int(cur.fetchone()[0])
    cur.execute("SELECT title FROM album WHERE artist_id = %s " %(artist_id))
    for row in cur.fetchall():
        delete_album(row[0])
    cur.execute("SELECT name FROM song WHERE artist_id = %s " %(artist_id))
    for row in cur.fetchall():
        delete_song(row[0])
    cur.execute("DELETE FROM artist WHERE id = %s " %(artist_id))
    conn.commit()
    cur.close()
    conn.close()


def find_singles():
    single_str = input("Is it single?(yes/no)\n")
    single = False
    if single_str == "yes":
        single = True
    if single:
        conn = connect_to_db()
        cur = conn.cursor()
        cur.execute("SELECT song.name, artist.name FROM song, artist "
                    "WHERE song.single = %s AND song.artist_id = artist.id" %(single))
        print("SONG        |ARTIST   \n")
        for row in cur.fetchall():
            print(row[0]+ "  " +row[1])


def find_by_date():
    start_date = parse_date(input("Print start date of album(YYYY-MM-DD):\n"))
    end_date = parse_date(input("Print end date of album(YYYY-MM-DD):\n"))
    conn = connect_to_db()
    cur = conn.cursor()
    single = False
    cur.execute("SELECT song.name, artist.name, album.title, album.release "
                "FROM song, artist, album, song_album "
                "WHERE song.single = %s AND song.artist_id = artist.id AND "
                "album.release >= '%s' AND album.release <= '%s' AND "
                "album.id = song_album.album_id AND song.id = song_album.song_id "
                % (single, start_date.date(), end_date.date()))
    print("SONG        |ARTIST   |ALBUM   |ALBUM DATE ")
    for row in cur.fetchall():
        print(row[0] + "  " + row[1] + "  " + row[2] + "  " + row[3].isoformat())


def parse_date(string):
    year = int(string[0:4])
    month = int(string[5:7])
    day = int(string[8:10])
    return datetime.datetime(year, month, day)


def print_menu():
    """output menu"""
    print("1. Add Artist.")
    print("2. Add Song.")
    print("3. Add Album.")
    print("4. Delete Song.")
    print("5. Delete Album.")
    print("6. Delete Artist.")
    print("7. Find By Date.")
    print("8. Find By Single.")
    print("10. Exit.")


def program_cycle():
    while True:
        print_menu()
        item = int(input())
        print(item)
        if item == 1:
            insert_artist()
        elif item == 2:
            insert_song()
        elif item == 3:
            insert_album()
        elif item == 4:
            delete_song()
        elif item == 5:
            delete_album()
        elif item == 6:
            delete_artist()
        elif item == 7:
            find_by_date()
        elif item == 8:
            find_singles()
        elif item == 10:
            break


if __name__ == '__main__':
    program_cycle()
