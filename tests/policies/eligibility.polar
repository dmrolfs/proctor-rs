eligible(item, environment) if environment.location_code == 33;
eligible(_, env: TestEnvironment { custom: { cat: "Otis" } });
