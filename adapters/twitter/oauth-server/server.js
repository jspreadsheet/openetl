const express = require("express");
const axios = require("axios");
const fs = require("fs").promises;
const app = express();
const port = 3000;

const clientId = process.env.TWITTER_CLIENT_ID;
const clientSecret = process.env.TWITTER_CLIENT_SECRET;
const redirectUri = process.env.REDIRECT_URI;
const tokenFile = process.env.ACCESS_TOKEN_FILE || "./tokens.json";

app.get('/auth', (req, res) => {
  const authUrl = `https://twitter.com/i/oauth2/authorize?response_type=code&client_id=${clientId}&redirect_uri=${redirectUri}&scope=tweet.read%20tweet.write%20users.read%20offline.access&state=state123&code_challenge=challenge&code_challenge_method=plain`;
  res.redirect(authUrl);
});

app.get("/callback", async (req, res) => {
	const { code } = req.query;
	if (!code) return res.status(400).send("No code provided");

	let response;
	const authHeader = Buffer.from(`${clientId}:${clientSecret}`).toString(
		"base64"
	);

	try {
		response = await axios.post(
			"https://api.twitter.com/2/oauth2/token",
			new URLSearchParams({
				code,
				grant_type: "authorization_code",
				redirect_uri: redirectUri,
				code_verifier: "challenge",
			}).toString(),
			{
				headers: {
					"Content-Type": "application/x-www-form-urlencoded",
					Authorization: `Basic ${authHeader}`,
				},
			}
		);

		console.log("Tokens:", response.data);
	} catch (error) {
		console.error("Error:", error.response?.data || error.message);
	}

	try {
    if ( response && response.data ) {
      await fs.writeFile(tokenFile, JSON.stringify(response.data, null, 2));
    }
	} catch (err) {
		console.error("error to save token file ", err.message);
	}

	res.send("Authentication successful! Tokens saved.");
});

app.get('/refresh', async (req, res) => {
  try {
    let tokens;
    try {
      tokens = JSON.parse(await fs.readFile(tokenFile, 'utf8'));
    } catch (readError) {
      return res.status(500).send('No existing tokens found or file corrupted');
    }

    if (tokens.expires_at > Date.now() + 60000) {
      return res.json(tokens);
    }

    const authHeader = Buffer.from(`${clientId}:${clientSecret}`).toString('base64');

    const response = await axios.post(
      'https://api.twitter.com/2/oauth2/token',
      new URLSearchParams({
        refresh_token: tokens.refresh_token,
        grant_type: 'refresh_token',
        client_id: clientId,
      }).toString(),
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Authorization': `Basic ${authHeader}`,
        },
      }
    );

    const newTokens = {
      access_token: response.data.access_token,
      refresh_token: response.data.refresh_token || tokens.refresh_token,
      expires_at: Date.now() + (response.data.expires_in * 1000),
    };

    await fs.writeFile(tokenFile, JSON.stringify(newTokens, null, 2));
    console.log('Tokens refreshed successfully');
    res.json(newTokens);
  } catch (error) {
    console.error('Refresh error:', error.response?.data || error.message);
    const errorMessage = error.response?.data?.error_description || error.message;
    res.status(500).send(`Refresh failed: ${errorMessage}`);
  }
});

app.get("/tokens", async (req, res) => {
	try {
		const tokens = JSON.parse(await fs.readFile(tokenFile, "utf8"));
		if (tokens.expires_at < Date.now()) {
			return res.redirect("/refresh");
		}
		res.json(tokens);
	} catch (error) {
		res.status(500).send(`Error: ${error.message}`);
	}
});

app.listen(port, () => console.log(`OAuth server running on port ${port}`));
