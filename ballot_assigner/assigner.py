import random


def clean_list(items):
    """Trim whitespace, drop blanks, and drop duplicates (case-insensitive) while keeping the original order."""
    seen = set()
    cleaned = []
    for item in items:
        item = " ".join(str(item).split())
        key = item.casefold()
        if not item or key in seen:
            continue
        seen.add(key)
        cleaned.append(item)
    return cleaned


def assign_measures(measures, names, rng=None):
    """Randomly assign every ballot measure to exactly one name.

    Measures are shuffled and dealt out like cards to a shuffled list of names, so
    no measure is ever given out twice and nobody ends up with more than one measure
    above anyone else. Returns a list of (measure, name) tuples in the original
    measure order.
    """
    measures = clean_list(measures)
    names = clean_list(names)
    if not measures:
        raise ValueError("Enter at least one ballot measure.")
    if not names:
        raise ValueError("Enter at least one name.")

    rng = rng or random.SystemRandom()
    shuffled_measures = measures[:]
    shuffled_names = names[:]
    rng.shuffle(shuffled_measures)
    rng.shuffle(shuffled_names)

    assigned_to = {}
    for i, measure in enumerate(shuffled_measures):
        assigned_to[measure] = shuffled_names[i % len(shuffled_names)]

    return [(measure, assigned_to[measure]) for measure in measures]


def group_by_person(assignments, names):
    """Return {name: [measures]} in the original name order, including names that received nothing."""
    grouped = {name: [] for name in clean_list(names)}
    for measure, name in assignments:
        grouped.setdefault(name, []).append(measure)
    return grouped
