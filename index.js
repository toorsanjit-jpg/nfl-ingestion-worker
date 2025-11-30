import fetch from "node-fetch";
import dotenv from "dotenv";
dotenv.config();

const RAPID_KEY = process.env.RAPID_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

async function runIngestion() {
  console.log("----- Cycle -----", new Date().toISOString());

  const scoreboard = await fetch(
    "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
  ).then((r) => r.json());

  const events = scoreboard?.events || [];
  const liveGames = events.filter((g) => {
    const comp = g.competitions?.[0];
    return comp?.status?.type?.state === "in";
  });

  if (liveGames.length === 0) {
    console.log("No live games right now.");
    return;
  }

  console.log("Live games:", liveGames.map((g) => g.id));

  for (const game of liveGames) {
    const gameId = game.id;

    try {
      const pbpRes = await fetch(
        `https://sports-information.p.rapidapi.com/nfl/play-by-play/${gameId}`,
        {
          headers: {
            "x-rapidapi-key": RAPID_KEY,
            "x-rapidapi-host": "sports-information.p.rapidapi.com"
          }
        }
      );

      if (!pbpRes.ok) {
        console.log("RapidAPI error", gameId, pbpRes.status);
        continue;
      }

      const pbpJson = await pbpRes.json();
      const plays = pbpJson?.allPlays || pbpJson?.plays || [];

      if (!Array.isArray(plays) || plays.length === 0) {
        console.log(`No plays returned for ${gameId}`);
        continue;
      }

      const mapped = plays.map((p) => ({
        game_id: Number(gameId),
        play_id: p.id ?? null,
        sequence_number: p.sequenceNumber ?? null,
        quarter: p.period ?? null,
        clock: p.clock?.displayValue ?? null,
        description: p.text ?? null,
        offense_team: p.team?.abbreviation ?? null,
        scoring_play: p.scoringPlay ?? false,
      }));

      const supabaseRes = await fetch(`${SUPABASE_URL}/rest/v1/nfl_plays`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          apikey: SUPABASE_SERVICE_KEY,
          Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
          Prefer: "resolution=merge-duplicates"
        },
        body: JSON.stringify(mapped)
      });

      console.log(`Upserted ${mapped.length} plays for game ${gameId}`);

    } catch (err) {
      console.log("Error:", err.message);
    }
  }
}

setInterval(runIngestion, 15000);
runIngestion();
