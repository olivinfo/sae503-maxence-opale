FROM python:3.11-slim

RUN useradd -m haddock
USER haddock
WORKDIR /home/haddock

COPY --chown=haddock:haddock requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=haddock:haddock app_users.py .
COPY --chown=haddock:haddock initial_data_users.csv .

ENV APP_PORT=5000

EXPOSE 5000

CMD ["python", "app_users.py"]
