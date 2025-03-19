const express = require('express');
const axios = require('axios');
const app = express();
const fs = require("fs").promises;

const CLIENT_ID = process.env.GITHUB_CLIENT_ID || 'your_client_id';
const CLIENT_SECRET = process.env.GITHUB_CLIENT_SECRET || 'your_client_secret';
const REDIRECT_URI = process.env.REDIRECT_URI;
const tokenFile = process.env.ACCESS_TOKEN_FILE || "./tokens.json";

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

app.get('/auth', (req, res) => {
  const authUrl = `https://github.com/login/oauth/authorize?client_id=${CLIENT_ID}&redirect_uri=${REDIRECT_URI}&scope=repo user`;
  res.redirect(authUrl);
});

app.get('/callback', async (req, res) => {
  const code = req.query.code;
  if (!code) {
    return res.status(400).send('No code provided');
  }

  try {
    const response = await axios.post('https://github.com/login/oauth/access_token', {
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code: code,
      redirect_uri: REDIRECT_URI,
    }, {
      headers: { 'Accept': 'application/json' },
    });

    try {
      if ( response && response.data ) {
        await fs.writeFile(tokenFile, JSON.stringify(response.data, null, 2));
        res.send('Authentication successful! Token stored. You can close this window.');
      }
    } catch (err) {
      console.error("error to save token file ", err.message);
      res.status(500).send('error ' + err.message);
    }
  } catch (error) {
    res.status(500).send(`Error fetching token: ${error.message}`);
  }
});

app.get("/tokens", async (req, res) => {
	try {
		const tokens = JSON.parse(await fs.readFile(tokenFile, "utf8"));
		res.json(tokens);
	} catch (error) {
		res.status(500).send(`Error: ${error.message}`);
	}
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`OAuth2 server running on port ${PORT}`));