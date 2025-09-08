FROM linuxserver/calibre


WORKDIR /app
COPY requirements.txt requirements.txt

# install packages
RUN pip3 install -r requirements.txt



COPY bottorrent.py /app/bottorrent.py
COPY root/ /

RUN chmod 777 /app/bottorrent.py 
RUN chmod 777 -R /etc/services.d/


VOLUME /books /config /mobi /imports

ENTRYPOINT ["/app/bottorrent.py"]