def positiveWitnessForDk(row):
    return None


def negativeWitnessForDk(row):
    return None


def validatorForDk(positive_witnesses, negative_witnesses):
    return True


def positiveWitnessForDd(row):
    return None


def negativeWitnessForDd(row):
    return None


def validatorForDd(positive_witnesses, negative_witnesses):
    return True


base_coin_functions = {"positiveWitnessForDk": positiveWitnessForDk,
                       "negativeWitnessForDk": negativeWitnessForDk,
                       "validatorForDk": validatorForDk,
                       "positiveWitnessForDd": positiveWitnessForDd,
                       "negativeWitnessForDd": negativeWitnessForDd,
                       "validatorForDd": validatorForDd}
