import psycopg2
import datetime


def connect_to_db():
    conn = psycopg2.connect(
        host='localhost', port='5432', database='lab1',
        user='user1', password='user1')

    return conn


def insert_artist(artist=None, fans=None):
    if artist is None:
        artist = input("Print artist name\n")
    if fans is None:
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


def insert_album(album=None, artist=None, rait=None, date_str=None, fans=None):
    if album is None:
        album = input("Print album name\n")
    if rait is None:
        rait = float(input("Print album raiting\n"))
    if date_str is None:
        date_str = input("Print date (YYYY-MM-DD)\n")
    release = parse_date(date_str)
    conn = connect_to_db()
    cur = conn.cursor()
    if artist is None:
        artist = input("Print artist\n")
    cur.execute("SELECT id FROM artist WHERE name = '%s' " % (artist))
    if cur.rowcount == 0:
        insert_artist(artist, fans)
    cur.execute("SELECT max(id) FROM artist")
    artist_id = int(cur.fetchone()[0])
    cur.execute("SELECT max(id) FROM album")
    id = int(cur.fetchone()[0]) + 1
    cur.execute("INSERT INTO album(id,artist_id,title,raiting,release) VALUES (%s,%s,%s,%s,%s)",
                (id, artist_id, album, rait, release))
    conn.commit()
    cur.close()
    conn.close()


def generate_songs(songs=0, counter=0):
    while (counter <= songs):
        song = "song" + str(counter)
        artist = "artist" + str(counter)
        single = "no"
        album = "album" + str(0)
        fans = 9938
        rait = 7.6
        date = "2000-10-21"
        insert_song(song, artist, single, album, fans, rait, date)
        counter = counter + 1


def insert_song(song=None, artist=None, single_str=None, album=None, fans=None, rait=None, date_str=None):
    if song is None:
        song = input("Print song name\n")
    if artist is None:
        artist = input("Print artist\n")
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM artist WHERE name = '%s' " % (artist))
    if cur.rowcount == 0:
        insert_artist(artist, fans)
    if single_str is None:
        single_str = input("Is it single?(yes/no)\n")
    single = False

    if single_str == "yes":
        single = True

    if not single:
        if album is None:
            album = input("Print album name\n")
        cur.execute("SELECT id FROM album WHERE title = '%s' " % (album))
        if cur.rowcount == 0:
            insert_album(album, artist, rait, date_str, fans)

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
    cur.execute("SELECT title FROM album WHERE artist_id = %s " % (artist_id))
    for row in cur.fetchall():
        delete_album(row[0])
    cur.execute("SELECT name FROM song WHERE artist_id = %s " % (artist_id))
    for row in cur.fetchall():
        delete_song(row[0])
    cur.execute("DELETE FROM artist WHERE id = %s " % (artist_id))
    conn.commit()
    cur.close()
    conn.close()


def find_songs():
    start_date = parse_date(input("Print start date of album(YYYY-MM-DD):\n"))
    end_date = parse_date(input("Print end date of album(YYYY-MM-DD):\n"))
    conn = connect_to_db()
    cur = conn.cursor()
    single_str = input("Is it single?(yes/no)\n")
    single = False
    if single_str == "yes":
        single = True
    cur.execute("SELECT song.name, artist.name, album.title, album.release "
                "FROM song, artist, album, song_album "
                "WHERE song.single = %s AND song.artist_id = artist.id AND "
                "album.release >= '%s' AND album.release <= '%s' AND "
                "album.id = song_album.album_id AND song.id = song_album.song_id "
                % (single, start_date.date(), end_date.date()))
    for row in cur.fetchall():
        print(row)


def parse_date(string):
    year = int(string[0:4])
    month = int(string[5:7])
    day = int(string[8:10])
    return datetime.datetime(year, month, day)


def update_attribute():
    print("1.Rename song")
    print("2.Rename artist")
    print("3.Rename album")
    item = int(input())
    if item == 1:
        attr = "name"
        table = "song"
    elif item == 2:
        attr = "name"
        table = "artist"
    elif item == 3:
        attr = "title"
        table = "album"

    old_val = input("Print name of " + table + "\n")
    new_val = input("Print new value\n")
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("UPDATE " + table + " SET " + attr + " = %s "
                + " WHERE " + attr + " = %s ", (new_val, old_val))
    conn.commit()


def search():
    type = int(input("1.Word no included\n"
                     "2.Search phrase\n"))
    item = input("Print str for searching:\n")
    conn = connect_to_db()
    cur = conn.cursor()
    if type == 2:
        cur.execute("SELECT lyrics FROM song  WHERE to_tsvector('russian', lyrics) @@ phraseto_tsquery('russian', '%s') "
                    % (item))
        print(cur.fetchall())
    if type == 1:
        cur.execute("SELECT lyrics FROM song  WHERE not to_tsvector('russian', lyrics) @@ to_tsquery('russian', '%s') "
                % (item))
        print(cur.fetchall())

def print_menu():
    """output menu"""
    print("1. Add Artist.")
    print("2. Add Song.")
    print("3. Add Album.")
    print("4. Delete Song.")
    print("5. Delete Album.")
    print("6. Delete Artist.")
    print("7. Find Songs.")
    print("8. Rename.")
    print("9. Search.")
    print("10. Exit.")


def program_cycle():
    #generate_songs(100, 10)
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
            find_songs()
        elif item == 8:
            update_attribute()
        elif item == 9:
            search()
        elif item == 10:
            break


if __name__ == '__main__':
    program_cycle()
