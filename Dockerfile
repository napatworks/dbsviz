FROM python:3.6.7-alpine
COPY requirement.txt requirement.txt
RUN apk --no-cache add --virtual .builddeps gcc gfortran musl-dev libxml2-dev libxslt-dev g++ subversion make automake
RUN pip install -r requirement.txt
EXPOSE 5000
COPY . .
RUN pip install pandas-datareader==0.6.0 -t lib/
RUN cd lib/pandas_datareader \
    && sed -ie 's/pandas.core.common/pandas.api.types/g' fred.py \
    && cd ../..
# CMD python main.py