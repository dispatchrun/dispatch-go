//go:build !durable

package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/dispatchrun/dispatch-go"
	"github.com/dispatchrun/dispatch-go/dispatchhttp"
	"golang.org/x/exp/maps"
)

func main() {
	getRepo := dispatch.Func("getRepo", func(ctx context.Context, name string) (*dispatchhttp.Response, error) {
		return dispatchhttp.Get(context.Background(), "https://api.github.com/repos/dispatchrun/"+name)
	})

	getStargazers := dispatch.Func("getStargazers", func(ctx context.Context, url string) (*dispatchhttp.Response, error) {
		return dispatchhttp.Get(context.Background(), url)
	})

	reduceStargazers := dispatch.Func("reduceStargazers", func(ctx context.Context, stargazerURLs []string) ([]string, error) {
		responses, err := getStargazers.Gather(stargazerURLs)
		if err != nil {
			return nil, err
		}
		stargazers := map[string]struct{}{}
		for _, res := range responses {
			var stars []struct {
				Login string `json:"login"`
			}
			if err := json.Unmarshal(res.Body, &stars); err != nil {
				return nil, err
			}
			for _, star := range stars {
				stargazers[star.Login] = struct{}{}
			}
		}
		return maps.Keys(stargazers), nil
	})

	fanout := dispatch.Func("fanout", func(ctx context.Context, repoNames []string) ([]string, error) {
		responses, err := getRepo.Gather(repoNames)
		if err != nil {
			return nil, err
		}

		var stargazerURLs []string
		for _, res := range responses {
			var repo struct {
				StargazersURL string `json:"stargazers_url"`
			}
			if err := json.Unmarshal(res.Body, &repo); err != nil {
				return nil, err
			}
			stargazerURLs = append(stargazerURLs, repo.StargazersURL)
		}

		return reduceStargazers.Await(stargazerURLs)
	})

	endpoint, err := dispatch.New(getRepo, getStargazers, reduceStargazers, fanout)
	if err != nil {
		log.Fatalf("failed to create endpoint: %v", err)
	}

	go func() {
		if _, err := fanout.Dispatch(context.Background(), []string{"coroutine", "dispatch-py"}); err != nil {
			log.Fatalf("failed to dispatch call: %v", err)
		}
	}()

	if err := endpoint.ListenAndServe(); err != nil {
		log.Fatalf("failed to serve endpoint: %v", err)
	}
}
