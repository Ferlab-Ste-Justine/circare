# Dockerfile

FROM openjdk:11

USER root:root

RUN apt-get update -y && apt-get upgrade -y && apt install -y tesseract-ocr

WORKDIR /workdir

COPY ./classes/artifacts/circare_jar/circare.jar .

COPY ./tessdata tessdata

ENTRYPOINT ["java", "-jar", "circare.jar"]

# https://stackoverflow.com/questions/41185591/jar-file-with-arguments-in-docker