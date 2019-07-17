# Dockerfile

FROM openjdk:11

USER root:root

RUN apt-get update -y && apt-get upgrade -y && apt install -y tesseract-ocr

WORKDIR /workdir

COPY ./out/artifacts/clin_pdf_search_jar/clin-pdf-search.jar .

COPY ./nlp nlp

COPY ./tessdata tessdata

ENTRYPOINT ["java", "-jar", "clin-pdf-search.jar"]

# https://stackoverflow.com/questions/41185591/jar-file-with-arguments-in-docker