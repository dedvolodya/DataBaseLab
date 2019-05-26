import psycopg2
import datetime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Float, Date, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import func
from sqlalchemy import update

engine = create_engine("postgresql://user1:user1@localhost:5432/lab1")
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


class Album(Base):
    __tablename__ = 'album'

    id = Column(Integer, primary_key=True, nullable=False)
    artist_id = Column(Integer, ForeignKey("artist.id"), nullable=False)
    title = Column(String)
    raiting = Column(Float)
    release = Column(Date)

    def __repr__(self):
        return "<Album(title='%s', raiting='%s', release='%s')>" % (
            self.title, self.raiting, self.release)


class Artist(Base):
    __tablename__ = 'artist'

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String)
    fans = Column(Integer)

    def __repr__(self):
        return "<Artist(name='%s')>" % (
            self.name)


class Song(Base):
    __tablename__ = 'song'

    id = Column(Integer, primary_key=True, nullable=False)
    artist_id = Column(Integer, ForeignKey("artist.id"), nullable=False)
    name = Column(String)
    single = Column(Boolean)
    lyrics = Column(String)

    def __repr__(self):
        return "<Song(name='%s')>" % (
            self.name)


class SongAlbum(Base):
    __tablename__ = 'song_album'

    id = Column(Integer, primary_key=True, nullable=False)
    song_id = Column(Integer, ForeignKey("song.id"))
    album_id = Column(Integer, ForeignKey("album.id"))

    def __repr__(self):
        return "<SongAlbum(song_id='%s',album_id='%s')>" % (
            self.song_id, self.album_id)


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
    id = int(session.query(func.max(Artist.id)).first()[0]) + 1
    session.add(Artist(id=id, name=artist, fans=fans))
    session.commit()


def insert_album(album=None, artist=None, rait=None, date_str=None, fans=None):
    if album is None:
        album = input("Print album name\n")
    if rait is None:
        rait = float(input("Print album raiting\n"))
    if date_str is None:
        date_str = input("Print date (YYYY-MM-DD)\n")
    release = parse_date(date_str)
    if artist is None:
        artist = input("Print artist\n")
    if session.query(Artist.id).filter_by(name=artist).count() == 0:
        insert_artist(artist, fans)
    artist_id = int(session.query(func.max(Artist.id)).first()[0])
    id = int(session.query(func.max(Album.id)).first()[0]) + 1
    session.add(Album(id=id, artist_id=artist_id, title=album, raiting=rait, release=release))
    session.commit();


def generate_songs(songs=0, counter=0):
    while counter <= songs:
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
    if session.query(Artist.id).filter_by(name=artist).count() == 0:
        insert_artist(artist, fans)
    if single_str is None:
        single_str = input("Is it single?(yes/no)\n")
    single = False

    if single_str == "yes":
        single = True

    if not single:
        if album is None:
            album = input("Print album name\n")
        if session.query(Album.id).filter_by(title=album).count() == 0:
            insert_album(album, artist, rait, date_str, fans)

    artist_id = int(session.query(func.max(Artist.id)).first()[0])
    song_id = int(session.query(func.max(Song.id)).first()[0]) + 1
    album_id = int(session.query(func.max(Album.id)).first()[0])
    session.add(Song(id=song_id, artist_id=artist_id, name=song, single=single))
    col_id = int(session.query(func.max(SongAlbum.id)).first()[0]) + 1
    session.add(SongAlbum(id=col_id, song_id=song_id, album_id=album_id))
    session.commit()


def delete_song(song=None):
    if song is None:
        song = input("Print song name\n")
    if session.query(Song.id).filter_by(name=song).count() == 0:
        return
    song_id = int(session.query(Song.id).filter_by(name=song).first()[0])
    for val in session.query(SongAlbum).filter_by(song_id=song_id).all():
        session.delete(val)
    for val in session.query(Song).filter_by(id=song_id):
        session.delete(val)
    session.commit()


def delete_album(album=None):
    if album is None:
        album = input("Print album name\n")
    if session.query(Album.id).filter_by(title=album).count() == 0:
        return
    album_id = int(session.query(Album.id).filter_by(title=album).first()[0])

    for row in session.query(SongAlbum.song_id).filter(SongAlbum.album_id == album_id).all():
        for val in session.query(SongAlbum).filter_by(song_id=row[0]).all():
            session.delete(val)
        for val in session.query(Song).filter_by(id=row[0]).all():
            session.delete(val)

    for val in session.query(Album).filter_by(id=album_id).all():
        session.delete(val)
    session.commit()


def delete_artist():
    artist = input("Print atrist name\n")
    if session.query(Artist.id).filter_by(name=artist).count() == 0:
        return
    artist_id = int(session.query(Artist.id).filter_by(name=artist).first()[0])
    for row in session.query(Album.title).filter_by(artist_id=artist_id).all():
        delete_album(row[0])
    for row in session.query(Song.name).filter_by(artist_id=artist_id):
        delete_song(row[0])
    val = session.query(Artist).filter_by(id=artist_id).first()
    session.delete(val)
    session.commit()


def find_songs():
    start_date = parse_date(input("Print start date of album(YYYY-MM-DD):\n"))
    end_date = parse_date(input("Print end date of album(YYYY-MM-DD):\n"))
    single_str = input("Is it single?(yes/no)\n")
    single = False
    if single_str == "yes":
        single = True

    res = session.query(
        Song.name, Artist.name, Album.title, Album.release
    ).filter_by(
        single=single
    ).filter(
        Song.artist_id == Artist.id
    ).filter(
        Album.release >= start_date.date()
    ).filter(
        Album.release <= end_date.date()
    ).filter(
        Album.id == SongAlbum.album_id
    ).filter(
        Song.id == SongAlbum.song_id
    )
    for row in res.all():
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
    old_val = input("Print old value\n")
    new_val = input("Print new value\n")
    if item == 1:
        session.query(Song).filter_by(name=old_val).update({Song.name: new_val})
    elif item == 2:
        session.query(Artist).filter_by(name=old_val).update({Artist.name: new_val})
    elif item == 3:
        session.query(Album).filter_by(title=old_val).update({Album.title: new_val})
    session.commit()


def search():
    type = int(input("1.Word no included\n"
                     "2.Search phrase\n"))
    item = input("Print str for searching:\n")
    conn = connect_to_db()
    cur = conn.cursor()
    if type == 2:
        cur.execute(
            "SELECT lyrics FROM song  WHERE to_tsvector('russian', lyrics) @@ phraseto_tsquery('russian', '%s') "
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
    # generate_songs(100, 10)
    session.query(Album).filter(Album.title == '123').update({Album.title: "aa"})
    session.commit()
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
