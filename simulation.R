library(tidyverse)
library(furrr)

data_generation <- function(n, true_mu){
  rnorm(n, true_mu)
}

simulate_ <- function(n, true_mu, fun, ...){
  d <- data_generation(n, true_mu)
  fun(d)
}


# this function is the loop over the design matrix
# in this design it is the inner loop
simulate <- function(design, seed){
  # keep this future,
  # it enables a future topology where each worker might have multiple cores
  future_pmap(
    # go over each row of the design
    design, simulate_,
    # each row has the same needs the same seed with explicit repetition
    # providing a single seed would simply mean furrr is generating its own seeds 
    # based on the provided
    .options = furrr_options(seed = rep.int(list(seed), nrow(design))))
}

# wraped in a function
parallel_seeds <- function(n, seed = NULL) {
  if (is.null(seed))
    cli::cli_abort("{.arg seed} must be provided.")
  RNGkind("L'Ecuyer-CMRG")
  set.seed(seed)
  cli::cli_inform("RNG kind set to {.val L'Ecuyer-CMRG} with {.arg seed = {seed}}.")
  purrr::accumulate(seq_len(n - 1), \(s, x)parallel::nextRNGStream(s), # x is discarded, only the prior seed is relevant.
                    .init = .Random.seed)
  
}

simulation <- expand_grid(n = 10:100, true_mu = c(0, .5, 1), fun = list(mean, median), # actual varying parameters
                          seed = parallel_seeds(n = 2, seed = 42)) # repetitions
# repetitions are parallel, rows in design sequential
plan(list(multisession, sequential))
# alternatively:
# plan(list(sequential, multisession))
# design conditions are parallel (more uneven runtimes make this suboptimal)
# or
# plan(list(slurm, multisession))
# repetitions are parallel on different workers, each worker utilizes multiple cores
results <- simulation %>% 
  # we split the big matrix in chunks of identical design, with unique seed each
  nest(.by = seed, .key = "design") %>% 
  # here comes the outer loop over repetition
  # technically it is unnecessary to pass a long the seed both via option and explicitly
  # in the end the option is ignored
  mutate(result = future_map2(design, seed, simulate, .options = furrr_options(seed = seed))) %>% 
  # we unfurl everything
  unnest(c(design, result)) %>% 
  # here result is a single digit, but if the `fun` returns a list,
  # these are turned into columns
  unnest(result)
saveRDS(results, "results.rds")
