-- Databricks notebook source
SELECT Location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
FROM portfolio_project.default.covid_deaths
WHERE Location = 'United States'
ORDER BY 1, 2
--Looking at Total Cases vs Total Deaths
--Shows likelihood of dying if you contract covid in your country 


-- COMMAND ----------

--Looking at Total Cases vs Total Population
--Shows what percentage of population got Covid
SELECT Location,
       date, 
       population, 
        total_cases, 
       (total_cases/population)*100 as PercentPopulationInfected
FROM portfolio_project.default.covid_deaths
WHERE Location = 'United States'
ORDER BY 1, 2;

-- COMMAND ----------

--Looking at Countries with Highest infection Rate compared to Population

SELECT Location, population, MAX(total_cases) as HighestInfectionCount, MAX((total_cases/population))*100 as PercentPopulationInfected
FROM portfolio_project.default.covid_deaths
GROUP BY Location, population 
Order by PercentPopulationInfected DESC;

-- COMMAND ----------

--Showing Countries with Highest Death Count Per Population

SELECT Location, MAX (CAST(total_deaths as int)) as TotalDeathCount
FROM portfolio_project.default.covid_deaths
WHERE continent is not null
GROUP BY Location
ORDER BY TotalDeathCount DESC;



-- COMMAND ----------

-- DBTITLE 1,LET'S BREAK THINGS DOWN BY CONTINENT
--LET'S BREAK THINGS DOWN BY CONTINENT
--Showing the Continents with the highest death count per population

SELECT continent, MAX (CAST(total_deaths as int)) as TotalDeathCount
FROM portfolio_project.default.covid_deaths
WHERE continent is not null
GROUP BY continent
ORDER BY TotalDeathCount DESC

-- COMMAND ----------

--GLOBAL NUMBERS WITH DATES

SELECT date,
  SUM(new_cases) AS total_new_cases,
  SUM(CAST(new_deaths AS DOUBLE)) AS total_new_deaths,
  CASE
    WHEN SUM(new_cases) = 0 THEN 0.0
    ELSE (SUM(CAST(new_deaths AS DOUBLE)) / SUM(new_cases)) * 100.0
  END AS death_percentage
FROM portfolio_project.default.covid_deaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY date;




-- COMMAND ----------

--GLOBAL NUMBERS AGGREGATED 

SELECT
  SUM(new_cases) AS total_new_cases,
  SUM(CAST(new_deaths AS DOUBLE)) AS total_new_deaths,
  CASE
    WHEN SUM(new_cases) = 0 THEN 0.0
    ELSE (SUM(CAST(new_deaths AS DOUBLE)) / SUM(new_cases)) * 100.0
  END AS death_percentage
FROM portfolio_project.default.covid_deaths
WHERE continent IS NOT NULL

-- COMMAND ----------

--Looking at Total Population vs Vaccinations

SELECT dea.continent  AS Continent,
       dea.location AS Location,
       dea.date AS Date,
       dea.population AS Population,
       vac.new_vaccinations,
       SUM(CAST(vac.new_vaccinations AS int)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS Rolling_People_Vaccinated
       FROM portfolio_project.default.covid_deaths dea
JOIN portfolio_project.default.covid_vaccinations vac
ON dea.location = vac.location
WHERE dea.continent IS NOT NULL
AND dea.date = vac.date
ORDER BY 2, 3


-- COMMAND ----------

--USE CTE to Calculate Rolling Total of Vaccinations 

WITH PopvsVac (
    Continent,
    Location,
    Date,
    Population,
    New_Vaccinations,
    RollingPeopleVaccinated
) AS (
    SELECT
        dea.continent  AS Continent,
        dea.location   AS Location,
        dea.date       AS Date,
        dea.population AS Population,
        -- Make sure to normalize the data type and handle NULLs
        CAST(COALESCE(vac.new_vaccinations, 0) AS BIGINT) AS New_Vaccinations,
        SUM(CAST(COALESCE(vac.new_vaccinations, 0) AS BIGINT)) OVER (
            PARTITION BY dea.location
            ORDER BY dea.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS RollingPeopleVaccinated
    FROM portfolio_project.default.covid_deaths       AS dea
    JOIN portfolio_project.default.covid_vaccinations AS vac
      ON dea.location = vac.location
     AND dea.date     = vac.date
    WHERE dea.continent IS NOT NULL
)
SELECT *, (RollingPeopleVaccinated/population)*100 AS PercentPopulationVaccinated
FROM PopvsVac
ORDER BY Location, Date;

-- COMMAND ----------

--Creating View to store data for later for Tableau Visualizations

CREATE OR REPLACE VIEW portfolio_project.default.PercentPopulationVaccinated AS
SELECT
  dea.continent  AS Continent,
  dea.location   AS Location,
  dea.date       AS Date,
  CAST(dea.population AS BIGINT) AS Population,
  CAST(COALESCE(vac.new_vaccinations, 0) AS BIGINT) AS New_Vaccinations,
  SUM(CAST(COALESCE(vac.new_vaccinations, 0) AS BIGINT)) OVER (
    PARTITION BY dea.location
    ORDER BY dea.date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS RollingPeopleVaccinated,
  CASE
    WHEN dea.population > 0
      THEN (RollingPeopleVaccinated / Population) * 100.0
    ELSE NULL
  END AS PercentPopulationVaccinated
FROM portfolio_project.default.covid_deaths       AS dea
JOIN portfolio_project.default.covid_vaccinations AS vac
  ON dea.location = vac.location
 AND dea.date     = vac.date
WHERE dea.continent IS NOT NULL;


-- COMMAND ----------

