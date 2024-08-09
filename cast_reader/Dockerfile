FROM python:3

# Add the script to the Docker Image
ADD . /root/

WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt
# Give execution rights on the cron scripts
RUN chmod 0644 /root/get_date.sh

#Install Cron
RUN apt-get update
RUN apt-get -y install cron

# Add the cron job
RUN crontab -l | { cat; echo "*/20 * * * * bash /root/crawl_casts.sh"; } | crontab -

# Run the command on container startup
CMD cron