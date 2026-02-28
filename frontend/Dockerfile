FROM node:18-alpine

WORKDIR /app

COPY package*.json ./

RUN npm install

COPY . .

EXPOSE 3000

ENV VITE_PORT=3000

# Overridden by docker-compose usually, but included as fallback
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "3000"]
